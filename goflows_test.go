package goflows

import (
	"context"
	"github.com/stretchr/testify/require"
	"sync/atomic"
	"testing"
	"time"
)

const (
	busFirst EventBus = iota + 1
	busSecond
	busThird
	busFourth
)

const (
	eventFirst EventType = iota + 1
	eventSecond
	eventThird
	eventFourth
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

func TestBasic(t *testing.T) {
	t.Parallel()

	require := require.New(t)

	eh := new(InMemoryTransport)

	cq, err := NewEngine(eh)
	require.Nil(err)
	require.NotNil(cq)

	err = cq.RegisterBus(busFirst, WithBusName("high prio"), WithPartitions(2))
	require.Nil(err)

	err = cq.RegisterBus(busSecond, WithBusName("normal prio"))
	require.Nil(err)

	err = cq.RegisterBus(busThird, WithBusName("low prio"))
	require.Nil(err)

	err = cq.RegisterBus(busSecond, WithBusName("should fail"))
	require.NotNil(err)

	err = cq.RegisterBus(busThird, WithBusName("normal prio"))
	require.NotNil(err)

	err = cq.RegisterEvent(busFirst, eventFirst, WithEventName("event a/1"))
	require.Nil(err)

	err = cq.RegisterEvent(busSecond, eventSecond, WithEventName("event b/2"))
	require.Nil(err)

	err = cq.RegisterEvent(busFirst, eventThird, WithEventName("event c/1"))
	require.Nil(err)

	err = cq.RegisterEvent(busFirst, eventFirst, WithEventName("event a/1"))
	require.NotNil(err)

	err = cq.RegisterEvent(busThird, eventThird, WithEventName("event c/3"))
	require.Nil(err)

	err = cq.RegisterEvent(busFourth, eventThird, WithEventName("event c/4"))
	require.NotNil(err)

	err = cq.Start()
	require.Nil(err)

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

	_, err = Subscribe(cq, busFirst, eventFirst, cbf)
	require.Nil(err)

	_, err = Subscribe(cq, busSecond, eventSecond, cbf2)
	require.Nil(err)

	tot = cq.countSubscriptions()
	require.Equal(int64(2), tot)

	_, err = Subscribe(cq, busFirst, eventFourth, cbf) // Event type is not registered
	require.NotNil(err)

	for i := 0; i < 3; i++ {
		err = cq.Publish(context.Background(), newTestEvent(eventFirst))
		require.Nil(err)
	}

	err = cq.Publish(context.Background(), newTestEvent(eventSecond))
	require.Nil(err)

	err = cq.Stop()
	require.Nil(err)

	require.Equal(int64(3), counter)
	require.Equal(int64(1), counter2)
}

func TestRequest(t *testing.T) {
	t.Parallel()

	require := require.New(t)

	eh := new(InMemoryTransport)

	cq, err := NewEngine(eh)
	require.Nil(err)
	require.NotNil(cq)

	err = cq.RegisterBus(busFirst, WithBusName("high prio"), WithPartitions(2))
	require.Nil(err)

	err = cq.RegisterBus(busSecond, WithBusName("low prio"))
	require.Nil(err)

	err = cq.RegisterBus(busThird, WithBusName("normal prio"))
	require.Nil(err)

	err = cq.RegisterEvent(busFirst, eventFirst, WithEventName("event a"))
	require.Nil(err)

	err = cq.RegisterEvent(busSecond, eventSecond, WithEventName("event b"))
	require.Nil(err)

	err = cq.RegisterEvent(busThird, eventThird, WithEventName("event d"))
	require.Nil(err)

	err = cq.Start()
	require.Nil(err)

	cbf3 := func(ev Event) {
		te2 := newTestEvent(eventFirst)
		te2.Referrer = ev.GetID()
		cq.Publish(context.Background(), te2)
	}

	_, err = Subscribe(cq, busThird, eventThird, cbf3)
	require.Nil(err)

	te := newTestEvent(eventThird)

	tot1 := cq.countSubscriptions()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	nev, err := Request[Event](ctx, cq, busFirst, te, eventFirst)
	require.Nil(err)
	require.NotNil(nev)
	require.Equal(EventType(eventFirst), nev.GetType())
	require.Equal(te.ID, nev.GetReferrer())

	tot2 := cq.countSubscriptions()
	require.Equal(tot1, tot2)

	err = cq.Stop()
	require.Nil(err)
}

func TestMultipleBusForSameEvent(t *testing.T) {
	t.Parallel()

	require := require.New(t)

	eh := new(InMemoryTransport)

	cq, err := NewEngine(eh)
	require.Nil(err)
	require.NotNil(cq)

	err = cq.RegisterBus(busFirst, WithBusName("high prio"), WithPartitions(2))
	require.Nil(err)

	err = cq.RegisterBus(busSecond, WithBusName("low prio"))
	require.Nil(err)

	tot, err := cq.GetBusTypeFromEventType(eventFirst)
	require.Equal(0, len(tot))
	require.NotNil(err)

	err = cq.RegisterEvent(busFirst, eventFirst, WithEventName("event a"))
	require.Nil(err)

	tot, err = cq.GetBusTypeFromEventType(eventFirst)
	require.Equal(1, len(tot))
	require.Nil(err)

	err = cq.RegisterEvent(busSecond, eventFirst, WithEventName("event b"))
	require.Nil(err)

	tot, err = cq.GetBusTypeFromEventType(eventFirst)
	require.Equal(2, len(tot))
	require.Nil(err)

	err = cq.Start()
	require.Nil(err)

	var counter int64
	cbfc := func(ev Event) {
		atomic.AddInt64(&counter, 1)
	}

	_, err = Subscribe(cq, busFirst, eventFirst, cbfc)
	require.Nil(err)

	_, err = Subscribe(cq, busSecond, eventFirst, cbfc)
	require.Nil(err)

	te := newTestEvent(eventFirst)

	err = cq.Publish(context.Background(), te)
	require.Nil(err)

	err = cq.Stop()
	require.Nil(err)

	require.Equal(int64(2), counter)
}
