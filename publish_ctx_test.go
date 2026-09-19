package goflows

import (
	"context"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
)

// A subscriber that publishes on the bus it is consuming, while that bus is
// full, must get ctx.Err() back instead of deadlocking the dispatcher.
func TestSelfPublishOnFullBusHonoursContext(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		require := require.New(t)
		cq := buildEngineWithBus(t, WithBufferSize(1))

		inside := make(chan struct{})
		bufferFull := make(chan struct{})
		innerDone := make(chan struct{})
		var innerErr atomic.Value
		var calls int64
		_, err := Subscribe(cq, scopeBusHigh, scopeEvOrder, func(Event) {
			if atomic.AddInt64(&calls, 1) != 1 {
				return
			}
			close(inside)
			<-bufferFull
			// Virtual clock: the deadline fires as soon as everything is blocked.
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()
			innerErr.Store(cq.Publish(ctx, newScopeEvent(scopeEvOrder)))
			close(innerDone)
		})
		require.NoError(err)

		require.NoError(cq.Publish(context.Background(), newScopeEvent(scopeEvOrder))) // dispatcher takes it and parks
		<-inside
		require.NoError(cq.Publish(context.Background(), newScopeEvent(scopeEvOrder))) // fills the single slot
		close(bufferFull)

		// Wait for the inner Publish to give up on its own before stopping: Stop
		// would otherwise release it early with ErrBusClosed.
		<-innerDone
		require.NoError(cq.Stop())

		got, _ := innerErr.Load().(error)
		require.ErrorIs(got, context.DeadlineExceeded)
		require.Equal(int64(2), calls)
	})
}

// Publish with an already cancelled context fails fast and delivers nothing.
func TestPublishCancelledContextFails(t *testing.T) {
	t.Parallel()
	require := require.New(t)
	cq := buildEngineWithBus(t)

	var got int64
	_, err := Subscribe(cq, scopeBusHigh, scopeEvOrder, counterCallback(&got))
	require.NoError(err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(cq.Publish(ctx, newScopeEvent(scopeEvOrder)), context.Canceled)

	require.NoError(cq.Stop())
	require.Equal(int64(0), got)
}
