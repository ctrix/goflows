package goflows

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type untypedEvent struct{ BaseEvent } // zero BaseEvent: type never set

// NewBaseEvent fills id, type and timestamp; the referrer is empty.
func TestNewBaseEvent(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	before := time.Now()
	e := NewBaseEvent(scopeEvOrder)

	require.Equal(scopeEvOrder, e.GetType())
	require.Len(e.GetID(), 36)
	require.False(e.Time.Before(before))
	require.Empty(e.GetReferrer())
	require.Empty(e.GetName())
}

// IDs are UUID v7: unique and time ordered.
func TestBaseEventIDsAreOrdered(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	prev := NewBaseEvent(scopeEvOrder).GetID()
	for i := 0; i < 100; i++ {
		cur := NewBaseEvent(scopeEvOrder).GetID()
		require.Greater(cur, prev)
		prev = cur
	}
}

// The zero value has an invalid type; Publish must refuse it even when some
// event type happens to be registered with value 1.
func TestPublishUntypedEventFails(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	const firstIota EventType = 1 // what `iota + 1` gives to the first user constant

	cq, err := NewCQRSEngine(new(InMemoryEventHandler))
	require.NoError(err)
	require.NoError(cq.RegisterBus(scopeBusHigh))
	require.NoError(cq.RegisterEvent(scopeBusHigh, firstIota))
	require.NoError(cq.Start())

	var got int64
	_, err = cq.Subscribe(scopeBusHigh, firstIota, counterCallback(&got))
	require.NoError(err)

	e := &untypedEvent{}
	require.Equal(EventType(EventTypeInvalid), e.GetType())
	require.ErrorIs(cq.Publish(context.Background(), e), EEventTypeInvalid)

	require.NoError(cq.Stop())
	require.Equal(int64(0), got, "an untyped event must not be routed anywhere")
}

// The wire format: id, type, RFC 3339 timestamp, optional referrer, name.
func TestBaseEventJSON(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	e := NewBaseEvent(scopeEvOrder)
	e.Name = "order placed"
	e.Referrer = "req-1"

	raw, err := json.Marshal(e)
	require.NoError(err)

	var back BaseEvent
	require.NoError(json.Unmarshal(raw, &back))
	require.Equal(e.ID, back.ID)
	require.Equal(e.Type, back.Type)
	require.Equal("order placed", back.Name)
	require.Equal("req-1", back.Referrer)
	require.True(e.Time.Equal(back.Time))

	// no referrer -> key omitted
	raw, err = json.Marshal(NewBaseEvent(scopeEvOrder))
	require.NoError(err)
	require.NotContains(string(raw), "referrer")
}
