package goflows

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// callOrTimeout runs fn and fails the test if it does not return within a
// couple of seconds. It turns a deadlock into a test failure.
func callOrTimeout(t *testing.T, what string, fn func() error) error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- fn() }()
	select {
	case err := <-done:
		return err
	case <-time.After(2 * time.Second):
		t.Fatalf("%s blocked instead of returning", what)
		return nil
	}
}

func TestSubscribeBeforeStartWorks(t *testing.T) {
	t.Parallel()
	require := require.New(t)
	cq := buildScopedEngine(t)

	var got int64
	err := callOrTimeout(t, "Subscribe before Start", func() error {
		_, err := cq.Subscribe(scopeBusHigh, scopeEvOrder, counterCallback(&got))
		return err
	})
	require.NoError(err)

	require.NoError(cq.Start())
	require.NoError(cq.Publish(context.Background(), newScopeEvent(scopeEvOrder)))
	require.NoError(cq.Stop())

	require.Equal(int64(1), got)
}

func TestSubscribeAfterStopFails(t *testing.T) {
	t.Parallel()
	require := require.New(t)
	cq := newScopedEngine(t)
	sub, err := cq.Subscribe(scopeBusHigh, scopeEvOrder, counterCallback(new(int64)))
	require.NoError(err)
	require.NoError(cq.Stop())

	err = callOrTimeout(t, "Subscribe after Stop", func() error {
		_, err := cq.Subscribe(scopeBusHigh, scopeEvOrder, counterCallback(new(int64)))
		return err
	})
	require.ErrorIs(err, EEngineStopped)

	err = callOrTimeout(t, "Unsubscribe after Stop", sub.Unsubscribe)
	require.ErrorIs(err, EEngineStopped)
}

func TestPublishAfterStopFails(t *testing.T) {
	t.Parallel()
	require := require.New(t)
	cq := newScopedEngine(t)
	require.NoError(cq.Stop())

	var err error
	require.NotPanics(func() { err = cq.Publish(context.Background(), newScopeEvent(scopeEvOrder)) })
	require.ErrorIs(err, EEngineStopped)
}

func TestRegisterAfterStopFails(t *testing.T) {
	t.Parallel()
	require := require.New(t)
	cq := newScopedEngine(t)
	require.NoError(cq.Stop())

	require.ErrorIs(cq.RegisterBus(scopeBusGhost), EEngineStopped)
	require.ErrorIs(cq.RegisterEvent(scopeBusHigh, scopeEvOrder+1), EEngineStopped)
}

func TestStopBeforeStartDoesNotPanic(t *testing.T) {
	t.Parallel()
	require := require.New(t)
	cq := buildScopedEngine(t)

	var err error
	require.NotPanics(func() { err = cq.Stop() })
	require.NoError(err)

	// Once stopped, the engine stays stopped.
	require.ErrorIs(cq.Start(), EEngineStopped)
}

func TestStopTwiceDoesNotPanic(t *testing.T) {
	t.Parallel()
	require := require.New(t)
	cq := newScopedEngine(t)

	require.NoError(cq.Stop())

	var err error
	require.NotPanics(func() { err = cq.Stop() })
	require.NoError(err, "Stop is idempotent")
}

func TestStartTwiceFails(t *testing.T) {
	t.Parallel()
	require := require.New(t)
	cq := newScopedEngine(t)

	require.ErrorIs(cq.Start(), EEngineStarted)
	require.NoError(cq.Stop())
}

// Stop must still flush events already published: this is what every other
// test in the suite relies on.
func TestStopFlushesPendingEvents(t *testing.T) {
	t.Parallel()
	require := require.New(t)
	cq := newScopedEngine(t)

	var got int64
	_, err := cq.Subscribe(scopeBusHigh, scopeEvOrder, counterCallback(&got))
	require.NoError(err)
	for i := 0; i < 50; i++ {
		require.NoError(cq.Publish(context.Background(), newScopeEvent(scopeEvOrder)))
	}
	require.NoError(cq.Stop())

	require.Equal(int64(50), atomic.LoadInt64(&got))
}
