package goflows

import (
	"log/slog"
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

type scopeEvent struct{ Event }

func (e *scopeEvent) Init() { e.BaseInit() }

func newScopeEvent(t EventType) *scopeEvent {
	e := &scopeEvent{}
	e.BaseInit()
	e.Type = t
	return e
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

	eh := new(InMemoryEventHandler)
	eh.Initialize()
	eh.SetLogger(slog.New(slog.DiscardHandler))

	cq, err := NewCQRSEngine(eh)
	require.NoError(err)
	cq.SetLogger(slog.New(slog.DiscardHandler))

	require.NoError(cq.RegisterBus(scopeBusHigh))
	require.NoError(cq.RegisterBus(scopeBusLow))
	require.NoError(cq.RegisterEvent(scopeBusHigh, scopeEvOrder))
	require.NoError(cq.RegisterEvent(scopeBusLow, scopeEvOrder))
	require.NoError(cq.RegisterEvent(scopeBusHigh, scopeEvOnlyHigh))

	return cq
}

func counterCallback(n *int64) EventSubscriptionCallback {
	return func(EventInterface, any) { atomic.AddInt64(n, 1) }
}

// A subscriber on one bus must receive only the copies of the event that
// travel on that bus, not the ones delivered on other buses.
func TestSubscribeIsScopedToBus(t *testing.T) {
	t.Parallel()
	require := require.New(t)
	cq := newScopedEngine(t)

	var onHigh int64
	require.NoError(cq.Subscribe(scopeBusHigh, scopeEvOrder, counterCallback(&onHigh), nil))

	require.NoError(cq.Publish(newScopeEvent(scopeEvOrder))) // goes to both buses
	require.NoError(cq.Stop())                               // flushes the queues

	require.Equal(int64(1), onHigh, "subscriber on High must see the event exactly once")
}

// Two subscribers on two buses, each with its own callback: each sees its
// own copy and only that.
func TestSubscribeEachBusGetsItsOwnCopy(t *testing.T) {
	t.Parallel()
	require := require.New(t)
	cq := newScopedEngine(t)

	var onHigh, onLow int64
	require.NoError(cq.Subscribe(scopeBusHigh, scopeEvOrder, counterCallback(&onHigh), nil))
	require.NoError(cq.Subscribe(scopeBusLow, scopeEvOrder, counterCallback(&onLow), nil))

	require.NoError(cq.Publish(newScopeEvent(scopeEvOrder)))
	require.NoError(cq.Publish(newScopeEvent(scopeEvOrder)))
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
	err := cq.Subscribe(scopeBusGhost, scopeEvOrder, counterCallback(&got), nil)
	require.ErrorIs(err, EEventBusDoesntExists)

	require.NoError(cq.Publish(newScopeEvent(scopeEvOrder)))
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
	err := cq.Subscribe(scopeBusLow, scopeEvOnlyHigh, counterCallback(&got), nil)
	require.ErrorIs(err, EEventNotRegisteredOnBus)

	require.NoError(cq.Publish(newScopeEvent(scopeEvOnlyHigh)))
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
	require.NoError(cq.Subscribe(scopeBusHigh, scopeEvOrder, cb, nil))
	require.NoError(cq.Subscribe(scopeBusLow, scopeEvOrder, cb, nil))
	require.Equal(int64(2), cq.countSubscriptions())

	require.NoError(cq.Unsubscribe(scopeBusLow, scopeEvOrder, cb, nil))
	require.Equal(int64(1), cq.countSubscriptions())

	require.NoError(cq.Publish(newScopeEvent(scopeEvOrder)))
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
	blocker := func(EventInterface, any) {
		if atomic.AddInt64(&first, 1) == 1 {
			close(inside) // dispatcher is now inside the range loop
			<-release
		}
	}

	var b, c, d int64
	cbB, cbC, cbD := counterCallback(&b), counterCallback(&c), counterCallback(&d)

	require.NoError(cq.Subscribe(scopeBusHigh, scopeEvOrder, blocker, nil))
	require.NoError(cq.Subscribe(scopeBusHigh, scopeEvOrder, cbB, nil))
	require.NoError(cq.Subscribe(scopeBusHigh, scopeEvOrder, cbC, nil))
	require.NoError(cq.Subscribe(scopeBusHigh, scopeEvOrder, cbD, nil))

	require.NoError(cq.Publish(newScopeEvent(scopeEvOrder)))

	select {
	case <-inside:
	case <-time.After(2 * time.Second):
		t.Fatal("dispatcher never reached the blocking subscriber")
	}

	// The dispatcher is parked on the first subscriber. Remove the third one.
	require.NoError(cq.Unsubscribe(scopeBusHigh, scopeEvOrder, cbC, nil))
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
	stable := counterCallback(&got)
	require.NoError(cq.Subscribe(scopeBusHigh, scopeEvOrder, stable, nil))

	const rounds = 200
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < rounds; i++ {
			_ = cq.Publish(newScopeEvent(scopeEvOrder))
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < rounds; i++ {
			var n int64
			cb := counterCallback(&n)
			_ = cq.Subscribe(scopeBusHigh, scopeEvOrder, cb, nil)
			_ = cq.Unsubscribe(scopeBusHigh, scopeEvOrder, cb, nil)
		}
	}()

	wg.Wait()
	require.NoError(cq.Stop())

	require.Equal(int64(rounds), got, "the stable subscriber must see every event")
	require.Equal(int64(1), cq.countSubscriptions())
}
