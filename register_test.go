package goflows

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// failingBusHandler wraps the in-memory transport and fails Publish on one bus.
type failingBusHandler struct {
	EventHandlerInterface
	failOn EventBus
	err    error
}

func (f *failingBusHandler) Publish(btype EventBus, ev EventInterface) error {
	if btype == f.failOn {
		return f.err
	}
	return f.EventHandlerInterface.Publish(btype, ev)
}

// When an event is registered on several buses and one of them fails, the
// other buses must still receive it and the error must reach the caller.
func TestPublishDeliversToAllBusesAndReportsFailures(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	inner := new(InMemoryEventHandler)
	boom := errors.New("high bus is down")
	eh := &failingBusHandler{EventHandlerInterface: inner, failOn: scopeBusHigh, err: boom}

	cq, err := NewCQRSEngine(eh)
	require.NoError(err)

	// Registration order matters: the failing bus comes first.
	require.NoError(cq.RegisterBus(scopeBusHigh))
	require.NoError(cq.RegisterBus(scopeBusLow))
	require.NoError(cq.RegisterEvent(scopeBusHigh, scopeEvOrder))
	require.NoError(cq.RegisterEvent(scopeBusLow, scopeEvOrder))
	require.NoError(cq.Start())

	var onLow int64
	_, err = cq.Subscribe(scopeBusLow, scopeEvOrder, counterCallback(&onLow))
	require.NoError(err)

	err = cq.Publish(newScopeEvent(scopeEvOrder))
	require.ErrorIs(err, boom)
	require.NoError(cq.Stop())

	require.Equal(int64(1), onLow, "the healthy bus must still receive the event")
}

// Concurrent RegisterEvent calls for the same type on different buses must
// all be recorded.
func TestRegisterEventConcurrentSameType(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	const buses = 16
	for round := 0; round < 20; round++ {
		eh := new(InMemoryEventHandler)
		cq, err := NewCQRSEngine(eh)
		require.NoError(err)

		for i := 0; i < buses; i++ {
			require.NoError(cq.RegisterBus(scopeBusHigh + EventBus(i)))
		}

		start := make(chan struct{})
		var wg sync.WaitGroup
		errs := make(chan error, buses)
		for i := 0; i < buses; i++ {
			wg.Add(1)
			go func(b EventBus) {
				defer wg.Done()
				<-start
				errs <- cq.RegisterEvent(b, scopeEvOrder)
			}(scopeBusHigh + EventBus(i))
		}
		close(start)
		wg.Wait()
		close(errs)
		for err := range errs {
			require.NoError(err)
		}

		got, err := cq.GetBusTypeFromEventType(scopeEvOrder)
		require.NoError(err)
		require.Len(got, buses, fmt.Sprintf("round %d lost registrations", round))
		require.NoError(cq.Stop())
	}
}
