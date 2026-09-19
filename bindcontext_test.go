package goflows

import (
	"context"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"
)

// A subscription bound to a context is removed when the context is done.
// synctest gives a virtual clock: Wait blocks until every goroutine in the
// bubble, including the one AfterFunc starts, is idle.
func TestBindContextUnsubscribesWhenDone(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		require := require.New(t)
		cq := newScopedEngine(t)

		var got int64
		ctx, cancel := context.WithCancel(context.Background())
		sub, err := Subscribe(cq, scopeBusHigh, scopeEvOrder, counterCallback(&got))
		require.NoError(err)
		require.Same(sub, sub.BindContext(ctx))

		require.NoError(cq.Publish(context.Background(), newScopeEvent(scopeEvOrder)))
		synctest.Wait() // delivered
		require.Equal(int64(1), cq.countSubscriptions())

		cancel()
		synctest.Wait() // AfterFunc ran
		require.Equal(int64(0), cq.countSubscriptions())

		require.NoError(cq.Publish(context.Background(), newScopeEvent(scopeEvOrder)))
		require.NoError(cq.Stop())
		require.Equal(int64(1), got, "nothing is delivered after the context is done")
	})
}

// Binding an already done context removes the subscription right away, and
// unsubscribing by hand first is fine: the AfterFunc finds nothing to remove.
func TestBindContextEdgeCases(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		require := require.New(t)
		cq := newScopedEngine(t)

		done, cancel := context.WithCancel(context.Background())
		cancel()
		sub, err := Subscribe(cq, scopeBusHigh, scopeEvOrder, counterCallback(new(int64)))
		require.NoError(err)
		sub.BindContext(done)
		synctest.Wait()
		require.Equal(int64(0), cq.countSubscriptions())

		ctx, cancel2 := context.WithCancel(context.Background())
		sub2, err := Subscribe(cq, scopeBusHigh, scopeEvOrder, counterCallback(new(int64)))
		require.NoError(err)
		sub2.BindContext(ctx)
		require.NoError(sub2.Unsubscribe())
		cancel2()
		synctest.Wait()
		require.Equal(int64(0), cq.countSubscriptions())

		require.NoError(cq.Stop())
	})
}
