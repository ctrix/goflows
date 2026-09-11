package goflows

import (
	"bytes"
	"io"
	"log/slog"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// With no logger given, the library must not write anything to stdout.
// Not parallel: it swaps os.Stdout.
func TestDefaultLoggerIsSilent(t *testing.T) {
	require := require.New(t)

	orig := os.Stdout
	r, w, err := os.Pipe()
	require.NoError(err)
	os.Stdout = w
	defer func() { os.Stdout = orig }()

	var buf bytes.Buffer
	done := make(chan struct{})
	go func() { _, _ = io.Copy(&buf, r); close(done) }()

	cq, err := NewCQRSEngine(new(InMemoryEventHandler))
	require.NoError(err)
	require.NoError(cq.RegisterBus(scopeBusHigh, WithBusName("orders")))
	require.NoError(cq.RegisterEvent(scopeBusHigh, scopeEvOrder, WithEventName("order")))
	require.NoError(cq.Start())
	require.NoError(cq.Subscribe(scopeBusHigh, scopeEvOrder, func(EventInterface, any) {}, nil))
	require.NoError(cq.Publish(newScopeEvent(scopeEvOrder)))
	require.Error(cq.Publish(newScopeEvent(scopeEvOnlyHigh))) // unregistered: error path logs too
	require.NoError(cq.Stop())

	os.Stdout = orig
	require.NoError(w.Close())
	<-done

	require.Empty(buf.String(), "library wrote to stdout by default")
}

// The logger given to the engine must be used by the engine and handed to
// the transport.
func TestWithLoggerReachesEngineAndTransport(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	rec := newRecordingHandler()
	cq, err := NewCQRSEngine(new(InMemoryEventHandler), WithLogger(slog.New(rec)))
	require.NoError(err)
	require.NoError(cq.RegisterBus(scopeBusHigh))

	_, ok := rec.find("library", LIBRARY_NAME)
	require.True(ok, "engine records must carry the library attribute")
	_, ok = rec.find("handler", INMEMORY_HANDLER_NAME)
	require.True(ok, "transport records must carry the handler attribute")

	require.NoError(cq.Stop())
}

// WithLogger(nil) is ignored: the engine stays silent instead of panicking.
func TestWithNilLoggerIsIgnored(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	cq, err := NewCQRSEngine(new(InMemoryEventHandler), WithLogger(nil))
	require.NoError(err)
	require.NoError(cq.RegisterBus(scopeBusHigh))
	require.NoError(cq.Stop())
}
