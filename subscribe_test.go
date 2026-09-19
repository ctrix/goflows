package goflows

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	scopeBusHigh EventBus = iota + 100
	scopeBusLow
	scopeBusGhost // never registered
)

const (
	scopeEvOrder EventType = iota + 500
	scopeEvOnlyHigh
)

type scopeEvent struct{ BaseEvent }

func newScopeEvent(t EventType) *scopeEvent {
	return &scopeEvent{BaseEvent: NewBaseEvent(t)}
}

// newScopedEngine builds and starts an engine with two buses. scopeEvOrder is
// registered on both buses, scopeEvOnlyHigh only on scopeBusHigh.
func newScopedEngine(t *testing.T) *CQRS {
	t.Helper()
	cq := buildScopedEngine(t)
	require.NoError(t, cq.Start())
	return cq
}

// buildScopedEngine is newScopedEngine without the Start call.
func buildScopedEngine(t *testing.T) *CQRS {
	t.Helper()
	require := require.New(t)

	eh := new(InMemoryTransport)

	cq, err := NewCQRSEngine(eh)
	require.NoError(err)

	require.NoError(cq.RegisterBus(scopeBusHigh))
	require.NoError(cq.RegisterBus(scopeBusLow))
	require.NoError(cq.RegisterEvent(scopeBusHigh, scopeEvOrder))
	require.NoError(cq.RegisterEvent(scopeBusLow, scopeEvOrder))
	require.NoError(cq.RegisterEvent(scopeBusHigh, scopeEvOnlyHigh))

	return cq
}

func counterCallback(n *int64) EventSubscriptionCallback {
	return func(Event) { atomic.AddInt64(n, 1) }
}

// A subscriber on one bus must receive only the copies of the event that
// travel on that bus, not the ones delivered on other buses.
func TestSubscribeIsScopedToBus(t *testing.T) {
	t.Parallel()
	require := require.New(t)
	cq := newScopedEngine(t)

	var onHigh int64
	_, err := cq.Subscribe(scopeBusHigh, scopeEvOrder, counterCallback(&onHigh))
	require.NoError(err)

	require.NoError(cq.Publish(context.Background(), newScopeEvent(scopeEvOrder))) // goes to both buses
	require.NoError(cq.Stop())                                                     // flushes the queues

	require.Equal(int64(1), onHigh, "subscriber on High must see the event exactly once")
}

// Two subscribers on two buses, each with its own callback: each sees its
// own copy and only that.
func TestSubscribeEachBusGetsItsOwnCopy(t *testing.T) {
	t.Parallel()
	require := require.New(t)
	cq := newScopedEngine(t)

	var onHigh, onLow int64
	_, err := cq.Subscribe(scopeBusHigh, scopeEvOrder, counterCallback(&onHigh))
	require.NoError(err)
	_, err = cq.Subscribe(scopeBusLow, scopeEvOrder, counterCallback(&onLow))
	require.NoError(err)

	require.NoError(cq.Publish(context.Background(), newScopeEvent(scopeEvOrder)))
	require.NoError(cq.Publish(context.Background(), newScopeEvent(scopeEvOrder)))
	require.NoError(cq.Stop())

	require.Equal(int64(2), onHigh)
	require.Equal(int64(2), onLow)
}

// Subscribing on a bus that was never registered must be rejected, and the
// callback must never fire.
func TestSubscribeOnUnregisteredBusFails(t *testing.T) {
	t.Parallel()
	require := require.New(t)
	cq := newScopedEngine(t)

	var got int64
	_, err := cq.Subscribe(scopeBusGhost, scopeEvOrder, counterCallback(&got))
	require.ErrorIs(err, EEventBusDoesntExists)

	require.NoError(cq.Publish(context.Background(), newScopeEvent(scopeEvOrder)))
	require.NoError(cq.Stop())

	require.Equal(int64(0), got)
}

// Subscribing on an existing bus for an event type that is not registered on
// that bus must be rejected, and the callback must never fire.
func TestSubscribeOnBusWithoutThatEventFails(t *testing.T) {
	t.Parallel()
	require := require.New(t)
	cq := newScopedEngine(t)

	var got int64
	_, err := cq.Subscribe(scopeBusLow, scopeEvOnlyHigh, counterCallback(&got))
	require.ErrorIs(err, EEventNotRegisteredOnBus)

	require.NoError(cq.Publish(context.Background(), newScopeEvent(scopeEvOnlyHigh)))
	require.NoError(cq.Stop())

	require.Equal(int64(0), got)
}

// Unsubscribing on one bus must not remove the same callback subscribed on
// another bus.
func TestUnsubscribeIsScopedToBus(t *testing.T) {
	t.Parallel()
	require := require.New(t)
	cq := newScopedEngine(t)

	var got int64
	cb := counterCallback(&got)
	_, err := cq.Subscribe(scopeBusHigh, scopeEvOrder, cb)
	require.NoError(err)
	low, err := cq.Subscribe(scopeBusLow, scopeEvOrder, cb)
	require.NoError(err)
	require.Equal(int64(2), cq.countSubscriptions())

	require.NoError(low.Unsubscribe())
	require.Equal(int64(1), cq.countSubscriptions())

	require.NoError(cq.Publish(context.Background(), newScopeEvent(scopeEvOrder)))
	require.NoError(cq.Stop())

	require.Equal(int64(1), got, "only the High subscription must remain")
}

// Unsubscribing while a dispatcher is iterating over the subscriber list must
// not corrupt the list the dispatcher is reading. With an in-place delete the
// remaining subscribers get shifted under the iterator, so one of them is
// invoked twice and another is skipped.
func TestUnsubscribeDuringDispatchDoesNotCorruptDelivery(t *testing.T) {
	t.Parallel()
	require := require.New(t)
	cq := newScopedEngine(t)

	inside := make(chan struct{})
	release := make(chan struct{})
	var first int64
	blocker := func(Event) {
		if atomic.AddInt64(&first, 1) == 1 {
			close(inside) // dispatcher is now inside the range loop
			<-release
		}
	}

	var b, c, d int64
	cbB, cbC, cbD := counterCallback(&b), counterCallback(&c), counterCallback(&d)

	_, err := cq.Subscribe(scopeBusHigh, scopeEvOrder, blocker)
	require.NoError(err)
	_, err = cq.Subscribe(scopeBusHigh, scopeEvOrder, cbB)
	require.NoError(err)
	subC, err := cq.Subscribe(scopeBusHigh, scopeEvOrder, cbC)
	require.NoError(err)
	_, err = cq.Subscribe(scopeBusHigh, scopeEvOrder, cbD)
	require.NoError(err)

	require.NoError(cq.Publish(context.Background(), newScopeEvent(scopeEvOrder)))

	select {
	case <-inside:
	case <-time.After(2 * time.Second):
		t.Fatal("dispatcher never reached the blocking subscriber")
	}

	// The dispatcher is parked on the first subscriber. Remove the third one.
	require.NoError(subC.Unsubscribe())
	close(release)

	require.NoError(cq.Stop())

	require.Equal(int64(1), b, "B must be delivered exactly once")
	require.Equal(int64(1), d, "D must be delivered exactly once")
	require.LessOrEqual(c, int64(1), "C may be delivered at most once")
}

// Concurrent publish, subscribe and unsubscribe must be race free. This test
// only has teeth under -race.
func TestConcurrentSubscribeUnsubscribePublish(t *testing.T) {
	t.Parallel()
	require := require.New(t)
	cq := newScopedEngine(t)

	var got int64
	_, err := cq.Subscribe(scopeBusHigh, scopeEvOrder, counterCallback(&got))
	require.NoError(err)

	const rounds = 200
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < rounds; i++ {
			_ = cq.Publish(context.Background(), newScopeEvent(scopeEvOrder))
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < rounds; i++ {
			var n int64
			if sub, err := cq.Subscribe(scopeBusHigh, scopeEvOrder, counterCallback(&n)); err == nil {
				_ = sub.Unsubscribe()
			}
		}
	}()

	wg.Wait()
	require.NoError(cq.Stop())

	require.Equal(int64(rounds), got, "the stable subscriber must see every event")
	require.Equal(int64(1), cq.countSubscriptions())
}
