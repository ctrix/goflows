package goflows

import (
	"context"
	"github.com/stretchr/testify/require"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"
)

const (
	BUS_FIRST = iota + 1
	BUS_SECOND
	BUS_THIRD
	BUS_FOURTH
	EVENT_FIRST
	EVENT_SECOND
	EVENT_THIRD
	EVENT_FOURTH
)

type TestEvent struct {
	BaseEvent
	TestInt int
}

func newTestEvent(typ EventType) *TestEvent {
	te := &TestEvent{BaseEvent: NewBaseEvent(typ), TestInt: 1}
	te.Name = "New Event"
	return te
}

type TestEventHandler struct {
}

func (eh *TestEventHandler) Initialize() error {
	return nil
}

func TestBasic(t *testing.T) {
	t.Parallel()

	// assert := require.New(t)
	require := require.New(t)

	// **************************************
	eh := new(InMemoryTransport)

	// **************************************
	cq, err := NewCQRSEngine(eh)
	require.Nil(err)
	require.NotNil(cq)

	// **************************************
	err = cq.RegisterBus(BUS_FIRST, WithBusName("high prio"), WithPartitions(2))
	require.Nil(err)

	err = cq.RegisterBus(BUS_SECOND, WithBusName("normal prio"))
	require.Nil(err)

	err = cq.RegisterBus(BUS_THIRD, WithBusName("low prio"))
	require.Nil(err)

	err = cq.RegisterBus(BUS_SECOND, WithBusName("should fail"))
	require.NotNil(err)

	err = cq.RegisterBus(BUS_THIRD, WithBusName("normal prio"))
	require.NotNil(err)

	// **************************************
	err = cq.RegisterEvent(BUS_FIRST, EVENT_FIRST, WithEventName("event a/1"))
	require.Nil(err)

	err = cq.RegisterEvent(BUS_SECOND, EVENT_SECOND, WithEventName("event b/2"))
	require.Nil(err)

	err = cq.RegisterEvent(BUS_FIRST, EVENT_THIRD, WithEventName("event c/1"))
	require.Nil(err)

	err = cq.RegisterEvent(BUS_FIRST, EVENT_FIRST, WithEventName("event a/1"))
	require.NotNil(err)

	err = cq.RegisterEvent(BUS_THIRD, EVENT_THIRD, WithEventName("event c/3"))
	require.Nil(err)

	err = cq.RegisterEvent(BUS_FOURTH, EVENT_THIRD, WithEventName("event c/4"))
	require.NotNil(err)

	// **************************************

	err = cq.Start()
	require.Nil(err)

	// **************************************
	var counter int64 = 0
	cbf := func(ev Event) {
		atomic.AddInt64(&counter, 1)
	}

	var counter2 int64 = 0
	cbf2 := func(ev Event) {
		atomic.AddInt64(&counter2, 1)
	}

	var tot int64
	tot = cq.countSubscriptions()
	require.Equal(int64(0), tot)

	_, err = cq.Subscribe(BUS_FIRST, EVENT_FIRST, cbf)
	require.Nil(err)

	_, err = cq.Subscribe(BUS_SECOND, EVENT_SECOND, cbf2)
	require.Nil(err)

	tot = cq.countSubscriptions()
	require.Equal(int64(2), tot)

	_, err = cq.Subscribe(BUS_FIRST, EVENT_FOURTH, cbf) // Event type is not registered
	require.NotNil(err)

	// **************************************

	for i := 0; i < 3; i++ {
		err = cq.Publish(context.Background(), newTestEvent(EVENT_FIRST))
		require.Nil(err)
	}

	err = cq.Publish(context.Background(), newTestEvent(EVENT_SECOND))
	require.Nil(err)

	// **************************************
	// When you unsubscribe, you are not sure where the current event stream has arrived, so it's useless to count the messages if you don't
	// know how many of them have been processed.
	// cq.Unsubscribe(EVENT_FIRST, cbf, &counter)
	// tot = cq.countSubscriptions()
	// require.Equal(int64(1), tot)

	// cq.Unsubscribe(EVENT_SECOND, cbf2, &counter2)
	// tot = cq.countSubscriptions()
	// require.Equal(int64(1), tot)

	// time.Sleep(510 * time.Millisecond)

	// **************************************
	err = cq.Stop()
	require.Nil(err)

	require.Equal(int64(3), counter)
	require.Equal(int64(1), counter2)
}

func TestRequest(t *testing.T) {
	t.Parallel()

	require := require.New(t)

	// **************************************
	eh := new(InMemoryTransport)

	// **************************************
	cq, err := NewCQRSEngine(eh)
	require.Nil(err)
	require.NotNil(cq)

	// **************************************
	err = cq.RegisterBus(BUS_FIRST, WithBusName("high prio"), WithPartitions(2))
	require.Nil(err)

	err = cq.RegisterBus(BUS_SECOND, WithBusName("low prio"))
	require.Nil(err)

	err = cq.RegisterBus(BUS_THIRD, WithBusName("normal prio"))
	require.Nil(err)

	// **************************************
	err = cq.RegisterEvent(BUS_FIRST, EVENT_FIRST, WithEventName("event a"))
	require.Nil(err)

	err = cq.RegisterEvent(BUS_SECOND, EVENT_SECOND, WithEventName("event b"))
	require.Nil(err)

	err = cq.RegisterEvent(BUS_THIRD, EVENT_THIRD, WithEventName("event d"))
	require.Nil(err)

	// **************************************

	err = cq.Start()
	require.Nil(err)

	// **************************************
	cbf3 := func(ev Event) {
		// slog.Info("======================================================= CB listener (pub&wait)")
		te2 := newTestEvent(EVENT_FIRST)
		te2.Referrer = ev.GetID()
		cq.Publish(context.Background(), te2)
	}

	_, err = cq.Subscribe(BUS_THIRD, EVENT_THIRD, cbf3)
	require.Nil(err)

	te := newTestEvent(EVENT_THIRD)

	tot1 := cq.countSubscriptions()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	nev, err := cq.Request(ctx, BUS_FIRST, te, EVENT_FIRST)
	require.Nil(err)
	require.NotNil(nev)
	require.Equal(EventType(EVENT_FIRST), nev.GetType())
	require.Equal(te.ID, nev.GetReferrer())

	tot2 := cq.countSubscriptions()
	require.Equal(tot1, tot2)

	// **************************************

	err = cq.Stop()
	require.Nil(err)
}

func TestMultipleBusForSameEvent(t *testing.T) {
	t.Parallel()

	require := require.New(t)

	// **************************************
	eh := new(InMemoryTransport)

	// **************************************
	cq, err := NewCQRSEngine(eh)
	require.Nil(err)
	require.NotNil(cq)

	// **************************************
	err = cq.RegisterBus(BUS_FIRST, WithBusName("high prio"), WithPartitions(2))
	require.Nil(err)

	err = cq.RegisterBus(BUS_SECOND, WithBusName("low prio"))
	require.Nil(err)

	// **************************************
	tot, err := cq.GetBusTypeFromEventType(EVENT_FIRST)
	require.Equal(0, len(tot))
	require.NotNil(err)

	err = cq.RegisterEvent(BUS_FIRST, EVENT_FIRST, WithEventName("event a"))
	require.Nil(err)

	tot, err = cq.GetBusTypeFromEventType(EVENT_FIRST)
	require.Equal(1, len(tot))
	require.Nil(err)

	err = cq.RegisterEvent(BUS_SECOND, EVENT_FIRST, WithEventName("event b"))
	require.Nil(err)

	tot, err = cq.GetBusTypeFromEventType(EVENT_FIRST)
	require.Equal(2, len(tot))
	require.Nil(err)

	// **************************************
	err = cq.Start()
	require.Nil(err)

	// **************************************
	var counter int64
	cbfc := func(ev Event) {
		slog.Info("======================================================= CB double catcher")
		atomic.AddInt64(&counter, 1)
	}

	_, err = cq.Subscribe(BUS_FIRST, EVENT_FIRST, cbfc)
	require.Nil(err)

	_, err = cq.Subscribe(BUS_SECOND, EVENT_FIRST, cbfc)
	require.Nil(err)

	te := newTestEvent(EVENT_FIRST)

	err = cq.Publish(context.Background(), te)
	require.Nil(err)

	// **************************************

	err = cq.Stop()
	require.Nil(err)

	require.Equal(int64(2), counter)
}
