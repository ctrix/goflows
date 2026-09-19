package goflows

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const scopeEvReply EventType = 600

// newRequestEngine registers scopeEvOrder (the request) and scopeEvReply (the
// reply) on scopeBusHigh. If responder is not nil it is subscribed to the
// request type and called with a reply function.
func newRequestEngine(t *testing.T, responder func(req EventInterface, reply func(*scopeEvent))) *CQRS {
	t.Helper()
	require := require.New(t)
	cq := newScopedEngine(t)
	require.NoError(cq.RegisterEvent(scopeBusHigh, scopeEvReply))

	if responder != nil {
		_, err := cq.Subscribe(scopeBusHigh, scopeEvOrder, func(req EventInterface) {
			responder(req, func(r *scopeEvent) {
				r.Referrer = req.GetID()
				_ = cq.Publish(r)
			})
		})
		require.NoError(err)
	}

	return cq
}

// A request that times out must not leave its temporary reply subscription
// behind.
func TestPublishAndWaitTimeoutRemovesSubscription(t *testing.T) {
	t.Parallel()
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
			_, err := cq.PublishAndWait(scopeBusHigh, newScopeEvent(scopeEvOrder), scopeEvReply, 10*time.Millisecond)
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
}

// A request whose Publish fails must not leave its reply subscription behind.
func TestPublishAndWaitPublishErrorRemovesSubscription(t *testing.T) {
	t.Parallel()
	require := require.New(t)
	cq := newRequestEngine(t, nil)

	before := cq.countSubscriptions()

	// This event type is not registered anywhere: Publish must fail fast.
	const unregistered EventType = 9999
	_, err := cq.PublishAndWait(scopeBusHigh, newScopeEvent(unregistered), scopeEvReply, time.Second)
	require.ErrorIs(err, EEventTypeInvalid)

	require.Equal(before, cq.countSubscriptions(), "failed request leaked its subscription")
	require.NoError(cq.Stop())
}

// Two replies to the same request must not race with the caller reading the
// result. The first reply wins, the second is dropped.
func TestPublishAndWaitTwoRepliesFirstWins(t *testing.T) {
	t.Parallel()
	require := require.New(t)
	cq := newRequestEngine(t, func(req EventInterface, reply func(*scopeEvent)) {
		first := newScopeEvent(scopeEvReply)
		first.Name = "first"
		reply(first)
		second := newScopeEvent(scopeEvReply)
		second.Name = "second"
		reply(second)
	})

	before := cq.countSubscriptions()

	for i := 0; i < 50; i++ {
		res, err := cq.PublishAndWait(scopeBusHigh, newScopeEvent(scopeEvOrder), scopeEvReply, time.Second)
		require.NoError(err)
		require.Equal("first", res.GetName())
	}

	require.NoError(cq.Stop())
	require.Equal(before, cq.countSubscriptions())
}

// The happy path: request, reply, result carries the reply.
func TestPublishAndWaitReturnsReply(t *testing.T) {
	t.Parallel()
	require := require.New(t)
	cq := newRequestEngine(t, func(req EventInterface, reply func(*scopeEvent)) {
		r := newScopeEvent(scopeEvReply)
		r.Name = "pong"
		reply(r)
	})

	req := newScopeEvent(scopeEvOrder)
	res, err := cq.PublishAndWait(scopeBusHigh, req, scopeEvReply, time.Second)
	require.NoError(err)
	require.Equal("pong", res.GetName())
	ref := res.GetReferrer()
	require.NotNil(ref)
	require.Equal(req.GetID(), *ref)

	require.NoError(cq.Stop())
}
