package goflows

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// buildEngineWithBus returns a started engine with scopeEvOrder registered on
// scopeBusHigh, which is created with the given options.
func buildEngineWithBus(t *testing.T, opts ...BusOption) *CQRS {
	t.Helper()
	require := require.New(t)

	eh := new(InMemoryEventHandler)

	cq, err := NewCQRSEngine(eh)
	require.NoError(err)

	require.NoError(cq.RegisterBus(scopeBusHigh, opts...))
	require.NoError(cq.RegisterEvent(scopeBusHigh, scopeEvOrder))
	require.NoError(cq.Start())
	return cq
}

// A panicking subscriber must not take the bus, or the process, down with it.
// Other subscribers keep receiving events.
func TestPanicInCallbackDoesNotKillBus(t *testing.T) {
	t.Parallel()
	require := require.New(t)
	cq := buildEngineWithBus(t)

	var healthy int64
	_, err := cq.Subscribe(scopeBusHigh, scopeEvOrder, func(Event) { panic("boom") })
	require.NoError(err)
	_, err = cq.Subscribe(scopeBusHigh, scopeEvOrder, counterCallback(&healthy))
	require.NoError(err)

	for i := 0; i < 3; i++ {
		require.NoError(cq.Publish(context.Background(), newScopeEvent(scopeEvOrder)))
	}
	require.NoError(cq.Stop())

	require.Equal(int64(3), healthy)
}

// With a single partition, events on a bus are delivered in publish order.
func TestSinglePartitionPreservesOrder(t *testing.T) {
	t.Parallel()
	require := require.New(t)
	cq := buildEngineWithBus(t)

	var mu sync.Mutex
	var seen []string
	_, err := cq.Subscribe(scopeBusHigh, scopeEvOrder, func(ev Event) {
		mu.Lock()
		seen = append(seen, ev.GetName())
		mu.Unlock()
	})
	require.NoError(err)

	const n = 100
	want := make([]string, 0, n)
	for i := 0; i < n; i++ {
		ev := newScopeEvent(scopeEvOrder)
		ev.Name = string(rune('a' + i%26))
		want = append(want, ev.Name)
		require.NoError(cq.Publish(context.Background(), ev))
	}
	require.NoError(cq.Stop())

	require.Equal(want, seen)
}

// With two partitions, a subscriber stuck on one event must not stop the next
// event from being delivered on the other partition.
func TestPartitionsDeliverConcurrently(t *testing.T) {
	t.Parallel()
	require := require.New(t)
	cq := buildEngineWithBus(t, WithPartitions(2))

	firstIn := make(chan struct{})
	release := make(chan struct{})
	secondIn := make(chan struct{})
	var calls int64
	_, err := cq.Subscribe(scopeBusHigh, scopeEvOrder, func(Event) {
		switch atomic.AddInt64(&calls, 1) {
		case 1:
			close(firstIn)
			<-release
		case 2:
			close(secondIn)
		}
	})
	require.NoError(err)

	require.NoError(cq.Publish(context.Background(), newScopeEvent(scopeEvOrder)))
	<-firstIn
	require.NoError(cq.Publish(context.Background(), newScopeEvent(scopeEvOrder)))

	select {
	case <-secondIn:
	case <-time.After(2 * time.Second):
		close(release)
		t.Fatal("second event was not delivered while the first subscriber was blocked")
	}
	close(release)

	require.NoError(cq.Stop())
	require.Equal(int64(2), calls)
}

// Invalid option values are rejected at registration.
func TestPartitionsOptionValidation(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	eh := new(InMemoryEventHandler)
	cq, err := NewCQRSEngine(eh)
	require.NoError(err)

	require.ErrorIs(cq.RegisterBus(scopeBusHigh, WithPartitions(0)), EOptionInvalid)
	require.ErrorIs(cq.RegisterBus(scopeBusHigh, WithPartitions(-3)), EOptionInvalid)
	require.ErrorIs(cq.RegisterBus(scopeBusHigh, WithBufferSize(0)), EOptionInvalid)
	require.False(cq.handler.BusExists(scopeBusHigh), "a rejected bus must not be created")
	require.NoError(cq.Stop())
}

// Publishing while Stop runs must never panic: each Publish either succeeds or
// returns an error.
func TestPublishConcurrentWithStopDoesNotPanic(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	for round := 0; round < 20; round++ {
		cq := buildEngineWithBus(t)
		_, err := cq.Subscribe(scopeBusHigh, scopeEvOrder, func(Event) {})
		require.NoError(err)

		var wg sync.WaitGroup
		var panicked atomic.Bool
		for p := 0; p < 4; p++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() {
					if r := recover(); r != nil {
						panicked.Store(true)
					}
				}()
				for i := 0; i < 200; i++ {
					_ = cq.Publish(context.Background(), newScopeEvent(scopeEvOrder))
				}
			}()
		}
		time.Sleep(50 * time.Microsecond)
		require.NoError(cq.Stop())
		wg.Wait()
		require.False(panicked.Load(), "Publish panicked during Stop")
	}
}
