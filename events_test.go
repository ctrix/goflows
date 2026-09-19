package goflows

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type untypedEvent struct{ Event }

func (e *untypedEvent) Init() { e.BaseInit() } // forgets to set Type

// BaseInit must leave the event type invalid, not default it to 1.
func TestBaseInitLeavesTypeInvalid(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	e := &untypedEvent{}
	e.Init()

	require.Equal(EventType(EventTypeInvalid), e.GetType())
	require.NotEmpty(e.GetID())
	require.NotZero(e.TS)
}

// Publishing an event whose type was never set must fail, even when some
// event type happens to be registered with value 1.
func TestPublishUntypedEventFails(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	const firstIota EventType = 1 // what `iota + 1` gives to the first user constant

	eh := new(InMemoryEventHandler)
	cq, err := NewCQRSEngine(eh)
	require.NoError(err)

	require.NoError(cq.RegisterBus(scopeBusHigh))
	require.NoError(cq.RegisterEvent(scopeBusHigh, firstIota))
	require.NoError(cq.Start())

	var got int64
	_, err = cq.Subscribe(scopeBusHigh, firstIota, counterCallback(&got))
	require.NoError(err)

	e := &untypedEvent{}
	e.Init()
	err = cq.Publish(e)
	require.ErrorIs(err, EEventTypeInvalid)

	require.NoError(cq.Stop())
	require.Equal(int64(0), got, "an untyped event must not be routed anywhere")
}
