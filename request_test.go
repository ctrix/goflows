package goflows

import (
	"context"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
)

const scopeEvReply EventType = 600

// newRequestEngine registers scopeEvOrder (the request) and scopeEvReply (the
// reply) on scopeBusHigh. If responder is not nil it is subscribed to the
// request type and called with a reply function.
func newRequestEngine(t *testing.T, responder func(req Event, reply func(*scopeEvent))) *CQRS {
	t.Helper()
	require := require.New(t)
	cq := newScopedEngine(t)
	require.NoError(cq.RegisterEvent(scopeBusHigh, scopeEvReply))

	if responder != nil {
		_, err := cq.Subscribe(scopeBusHigh, scopeEvOrder, func(req Event) {
			responder(req, func(r *scopeEvent) {
				r.Referrer = req.GetID()
				_ = cq.Publish(context.Background(), r)
			})
		})
		require.NoError(err)
	}

	return cq
}

// A request that times out must not leave its temporary reply subscription
// behind.
func TestRequestTimeoutRemovesSubscription(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		require := require.New(t)
		cq := newRequestEngine(t, nil) // nobody answers

		before := cq.countSubscriptions()

		const n = 20
		var wg sync.WaitGroup
		errs := make(chan error, n)
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
				defer cancel()
				_, err := cq.Request(ctx, scopeBusHigh, newScopeEvent(scopeEvOrder), scopeEvReply)
				errs <- err
			}()
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			require.ErrorIs(err, context.DeadlineExceeded)
		}

		require.Equal(before, cq.countSubscriptions(), "timed out requests leaked subscriptions")
		require.NoError(cq.Stop())
	})
}

// A request whose Publish fails must not leave its reply subscription behind.
func TestRequestPublishErrorRemovesSubscription(t *testing.T) {
	t.Parallel()
	require := require.New(t)
	cq := newRequestEngine(t, nil)

	before := cq.countSubscriptions()

	// This event type is not registered anywhere: Publish must fail fast.
	const unregistered EventType = 9999
	_, err := cq.Request(context.Background(), scopeBusHigh, newScopeEvent(unregistered), scopeEvReply)
	require.ErrorIs(err, EEventTypeInvalid)

	require.Equal(before, cq.countSubscriptions(), "failed request leaked its subscription")
	require.NoError(cq.Stop())
}

// Two replies to the same request must not race with the caller reading the
// result. The first reply wins, the second is dropped.
func TestRequestTwoRepliesFirstWins(t *testing.T) {
	t.Parallel()
	require := require.New(t)
	cq := newRequestEngine(t, func(req Event, reply func(*scopeEvent)) {
		first := newScopeEvent(scopeEvReply)
		first.Name = "first"
		reply(first)
		second := newScopeEvent(scopeEvReply)
		second.Name = "second"
		reply(second)
	})

	before := cq.countSubscriptions()

	for i := 0; i < 50; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		res, err := cq.Request(ctx, scopeBusHigh, newScopeEvent(scopeEvOrder), scopeEvReply)
		cancel()
		require.NoError(err)
		require.Equal("first", res.GetName())
	}

	require.NoError(cq.Stop())
	require.Equal(before, cq.countSubscriptions())
}

// The happy path: request, reply, result carries the reply.
func TestRequestReturnsReply(t *testing.T) {
	t.Parallel()
	require := require.New(t)
	cq := newRequestEngine(t, func(req Event, reply func(*scopeEvent)) {
		r := newScopeEvent(scopeEvReply)
		r.Name = "pong"
		reply(r)
	})

	req := newScopeEvent(scopeEvOrder)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	res, err := cq.Request(ctx, scopeBusHigh, req, scopeEvReply)
	require.NoError(err)
	require.Equal("pong", res.GetName())
	require.Equal(req.GetID(), res.GetReferrer())

	require.NoError(cq.Stop())
}

// A request whose context is already cancelled fails with context.Canceled,
// distinct from a timeout, and leaves no subscription behind.
func TestRequestCancelledContext(t *testing.T) {
	t.Parallel()
	require := require.New(t)
	cq := newRequestEngine(t, nil)
	before := cq.countSubscriptions()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := cq.Request(ctx, scopeBusHigh, newScopeEvent(scopeEvOrder), scopeEvReply)
	require.ErrorIs(err, context.Canceled)
	require.NotErrorIs(err, context.DeadlineExceeded)

	require.Equal(before, cq.countSubscriptions())
	require.NoError(cq.Stop())
}
