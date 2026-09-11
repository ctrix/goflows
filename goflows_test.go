package goflows

import (
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
	Event
	TestInt int
}

func (t *TestEvent) Init() {
	t.BaseInit()
	t.Name = "New Event"
	t.Type = EVENT_FIRST

	t.TestInt = 1
	return
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
	eh := new(InMemoryEventHandler)

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
	cbf := func(ev EventInterface, cbdata any) {
		cnt := cbdata.(*int64)
		atomic.AddInt64(cnt, 1)
		// slog.Info("1>>> Subscription callback", "event-id", ev.GetID())
		// slog.Info("======================================================= 1", "counter", *cnt)
	}

	var counter2 int64 = 0
	cbf2 := func(ev EventInterface, cbdata any) {
		cnt := cbdata.(*int64)
		atomic.AddInt64(cnt, 1)
		// slog.Info("2>>> Subscription callback", "event-id", ev.GetID())
		// slog.Info("======================================================= 2", "counter", *cnt)
	}

	var tot int64
	tot = cq.countSubscriptions()
	require.Equal(int64(0), tot)

	err = cq.Subscribe(BUS_FIRST, EVENT_FIRST, cbf, &counter)
	require.Nil(err)

	err = cq.Subscribe(BUS_SECOND, EVENT_SECOND, cbf2, &counter2)
	require.Nil(err)

	tot = cq.countSubscriptions()
	require.Equal(int64(2), tot)

	cq.Unsubscribe(BUS_FIRST, EVENT_FIRST+9999, cbf2, &counter2) // Inexistant subscription
	tot = cq.countSubscriptions()
	require.Equal(int64(2), tot)

	err = cq.Subscribe(BUS_FIRST, EVENT_FOURTH, cbf, &counter) // Event type is not registered
	require.NotNil(err)

	// **************************************

	te := &TestEvent{}
	te.Init()
	err = cq.Publish(te)
	require.Nil(err)

	te = &TestEvent{}
	te.Init()
	err = cq.Publish(te)
	require.Nil(err)

	te = &TestEvent{}
	te.Init()
	err = cq.Publish(te)
	require.Nil(err)

	te = &TestEvent{}
	te.Init()
	te.Type = EVENT_SECOND
	err = cq.Publish(te)
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

func TestPublishAndWait(t *testing.T) {
	t.Parallel()

	require := require.New(t)

	// **************************************
	eh := new(InMemoryEventHandler)

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
	cbf3 := func(ev EventInterface, cbdata any) {
		// slog.Info("======================================================= CB listener (pub&wait)")
		te2 := &TestEvent{}
		te2.Init()
		te2.Referrer = ev.GetID()
		cq.Publish(te2)
	}

	err = cq.Subscribe(BUS_THIRD, EVENT_THIRD, cbf3, nil)
	require.Nil(err)

	te := &TestEvent{}
	te.Init()
	te.Type = EVENT_THIRD

	tot1 := cq.countSubscriptions()

	nev, err := cq.PublishAndWait(BUS_FIRST, te, EVENT_FIRST, 3*time.Second)
	require.Nil(err)
	require.NotNil(nev)
	require.Equal(EventType(EVENT_FIRST), nev.GetType())
	require.Equal(nev.GetReferrer(), &te.ID)

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
	eh := new(InMemoryEventHandler)

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
	cbfc := func(ev EventInterface, cbdata any) {
		slog.Info("======================================================= CB double catcher")
		atomic.AddInt64(&counter, 1)
	}

	err = cq.Subscribe(BUS_FIRST, EVENT_FIRST, cbfc, nil)
	require.Nil(err)

	err = cq.Subscribe(BUS_SECOND, EVENT_FIRST, cbfc, nil)
	require.Nil(err)

	te := &TestEvent{}
	te.Init()
	te.Type = EVENT_FIRST

	err = cq.Publish(te)
	require.Nil(err)

	// **************************************

	err = cq.Stop()
	require.Nil(err)

	require.Equal(int64(2), counter)
}
