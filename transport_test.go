package goflows

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"
)

// bareTransport implements Transport and nothing else: no SetLogger.
type bareTransport struct{ inner InMemoryTransport }

func (b *bareTransport) Name() string                             { return "bare" }
func (b *bareTransport) Open(bus EventBus, cfg BusConfig) error   { return b.inner.Open(bus, cfg) }
func (b *bareTransport) Has(bus EventBus) bool                    { return b.inner.Has(bus) }
func (b *bareTransport) Stream(bus EventBus) (<-chan Event, bool) { return b.inner.Stream(bus) }
func (b *bareTransport) Publish(ctx context.Context, bus EventBus, ev Event) error {
	return b.inner.Publish(ctx, bus, ev)
}
func (b *bareTransport) Close() error { return b.inner.Close() }

// A transport that does not implement SetLogger is fine: the engine only
// hands the logger over when the transport accepts it.
func TestTransportWithoutSetLogger(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	rec := newRecordingHandler()
	cq, err := NewCQRSEngine(&bareTransport{}, WithLogger(slog.New(rec)))
	require.NoError(err)
	require.NoError(cq.RegisterBus(scopeBusHigh))
	require.NoError(cq.RegisterEvent(scopeBusHigh, scopeEvOrder))
	require.NoError(cq.Start())

	var got int64
	_, err = cq.Subscribe(scopeBusHigh, scopeEvOrder, counterCallback(&got))
	require.NoError(err)
	require.NoError(cq.Publish(context.Background(), newScopeEvent(scopeEvOrder)))
	require.NoError(cq.Stop())

	require.Equal(int64(1), got)
	_, ok := rec.find("library", LIBRARY_NAME)
	require.True(ok, "engine still logs with the given logger")
}

// closeFailingTransport fails Close.
type closeFailingTransport struct {
	InMemoryTransport
	err error
}

func (c *closeFailingTransport) Close() error {
	_ = c.InMemoryTransport.Close()
	return c.err
}

// An error from the transport Close is returned by Stop, after the buses have
// been drained.
func TestStopReturnsTransportCloseError(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	boom := errors.New("broker unreachable")
	cq, err := NewCQRSEngine(&closeFailingTransport{err: boom})
	require.NoError(err)
	require.NoError(cq.RegisterBus(scopeBusHigh))
	require.NoError(cq.RegisterEvent(scopeBusHigh, scopeEvOrder))
	require.NoError(cq.Start())

	var got int64
	_, err = cq.Subscribe(scopeBusHigh, scopeEvOrder, counterCallback(&got))
	require.NoError(err)
	require.NoError(cq.Publish(context.Background(), newScopeEvent(scopeEvOrder)))

	require.ErrorIs(cq.Stop(), boom)
	require.Equal(int64(1), got, "queued events are still delivered")
	require.NoError(cq.Stop(), "second Stop is still a no-op")
}
