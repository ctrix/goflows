package goflows

import (
	"log/slog"
	"sync/atomic"
	"testing"

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

// newScopedEngine builds an engine with two buses. scopeEvOrder is registered
// on both buses, scopeEvOnlyHigh only on scopeBusHigh.
func newScopedEngine(t *testing.T) *CQRS {
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
	require.NoError(cq.Start())

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
