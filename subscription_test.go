package goflows

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

// Subscribe returns a handle; Unsubscribe on it stops delivery.
func TestSubscriptionHandleUnsubscribes(t *testing.T) {
	t.Parallel()
	require := require.New(t)
	cq := newScopedEngine(t)

	var got int64
	delivered := make(chan struct{}, 8)
	sub, err := cq.Subscribe(scopeBusHigh, scopeEvOrder, func(Event) {
		atomic.AddInt64(&got, 1)
		delivered <- struct{}{}
	})
	require.NoError(err)
	require.NotNil(sub)
	require.Equal(scopeBusHigh, sub.Bus())
	require.Equal(scopeEvOrder, sub.Type())

	require.NoError(cq.Publish(context.Background(), newScopeEvent(scopeEvOrder)))
	<-delivered // Publish is asynchronous: wait for the first delivery
	require.NoError(sub.Unsubscribe())
	require.Equal(int64(0), cq.countSubscriptions())
	require.NoError(cq.Publish(context.Background(), newScopeEvent(scopeEvOrder)))
	require.NoError(cq.Stop())

	require.Equal(int64(1), got, "only the event published before Unsubscribe is delivered")
}

// The same callback subscribed twice is two independent subscriptions.
func TestSameCallbackSubscribedTwiceIsCalledTwice(t *testing.T) {
	t.Parallel()
	require := require.New(t)
	cq := newScopedEngine(t)

	var got int64
	cb := counterCallback(&got)
	s1, err := cq.Subscribe(scopeBusHigh, scopeEvOrder, cb)
	require.NoError(err)
	s2, err := cq.Subscribe(scopeBusHigh, scopeEvOrder, cb)
	require.NoError(err)
	require.NotSame(s1, s2)
	require.Equal(int64(2), cq.countSubscriptions())

	require.NoError(cq.Publish(context.Background(), newScopeEvent(scopeEvOrder)))
	require.NoError(cq.Stop())

	require.Equal(int64(2), got)
}

// Unsubscribing a handle removes that subscription only.
func TestUnsubscribeRemovesOnlyItself(t *testing.T) {
	t.Parallel()
	require := require.New(t)
	cq := newScopedEngine(t)

	var a, b int64
	subA, err := cq.Subscribe(scopeBusHigh, scopeEvOrder, counterCallback(&a))
	require.NoError(err)
	_, err = cq.Subscribe(scopeBusHigh, scopeEvOrder, counterCallback(&b))
	require.NoError(err)

	require.NoError(subA.Unsubscribe())
	require.Equal(int64(1), cq.countSubscriptions())

	require.NoError(cq.Publish(context.Background(), newScopeEvent(scopeEvOrder)))
	require.NoError(cq.Stop())

	require.Equal(int64(0), a)
	require.Equal(int64(1), b)
}

// Unsubscribe is idempotent.
func TestUnsubscribeTwiceIsNoop(t *testing.T) {
	t.Parallel()
	require := require.New(t)
	cq := newScopedEngine(t)

	sub, err := cq.Subscribe(scopeBusHigh, scopeEvOrder, counterCallback(new(int64)))
	require.NoError(err)

	require.NoError(sub.Unsubscribe())
	require.NoError(sub.Unsubscribe())
	require.Equal(int64(0), cq.countSubscriptions())
	require.NoError(cq.Stop())
}

// A nil callback is rejected and yields no handle.
func TestSubscribeNilCallbackFails(t *testing.T) {
	t.Parallel()
	require := require.New(t)
	cq := newScopedEngine(t)

	sub, err := cq.Subscribe(scopeBusHigh, scopeEvOrder, nil)
	require.ErrorIs(err, ErrSubscriptionInvalid)
	require.Nil(sub)
	require.NoError(cq.Stop())
}
