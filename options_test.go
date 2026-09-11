package goflows

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// recordingHandler is a slog.Handler that keeps every record it receives.
// Handlers derived with WithAttrs share the same store.
type recordingHandler struct {
	attrs []slog.Attr
	store *recordStore
}

type recordStore struct {
	mu      sync.Mutex
	records []map[string]any
}

func newRecordingHandler() *recordingHandler {
	return &recordingHandler{store: &recordStore{}}
}

func (h *recordingHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *recordingHandler) Handle(_ context.Context, r slog.Record) error {
	m := map[string]any{"msg": r.Message}
	for _, a := range h.attrs {
		m[a.Key] = a.Value.Any()
	}
	r.Attrs(func(a slog.Attr) bool { m[a.Key] = a.Value.Any(); return true })
	h.store.mu.Lock()
	h.store.records = append(h.store.records, m)
	h.store.mu.Unlock()
	return nil
}
func (h *recordingHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &recordingHandler{attrs: append(append([]slog.Attr{}, h.attrs...), attrs...), store: h.store}
}
func (h *recordingHandler) WithGroup(string) slog.Handler { return h }

// find returns the first record whose attribute key has the given value.
func (h *recordingHandler) find(key string, value any) (map[string]any, bool) {
	h.store.mu.Lock()
	defer h.store.mu.Unlock()
	for _, r := range h.store.records {
		if r[key] == value {
			return r, true
		}
	}
	return nil, false
}

// Bus and event names given at registration must be kept and used in logs.
func TestNamesAreKeptAndLogged(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	rec := newRecordingHandler()
	cq, err := NewCQRSEngine(new(InMemoryEventHandler), WithLogger(slog.New(rec)))
	require.NoError(err)

	require.NoError(cq.RegisterBus(scopeBusHigh, WithBusName("orders")))
	require.NoError(cq.RegisterEvent(scopeBusHigh, scopeEvOrder, WithEventName("order placed")))

	r, ok := rec.find("bus-name", "orders")
	require.True(ok, "bus registration must log the bus name")
	require.Equal(scopeBusHigh, r["bus-type"])

	r, ok = rec.find("event-name", "order placed")
	require.True(ok, "event registration must log the event name")
	require.Equal(scopeEvOrder, r["event-type"])

	require.NoError(cq.Stop())
}

// The bus buffer size option must be honoured by the in-memory transport:
// with a buffer of one and a blocked subscriber, the third Publish blocks.
func TestBufferSizeIsHonoured(t *testing.T) {
	t.Parallel()
	require := require.New(t)
	cq := buildEngineWithBus(t, WithBufferSize(1))

	inside := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	require.NoError(cq.Subscribe(scopeBusHigh, scopeEvOrder, func(EventInterface, any) {
		once.Do(func() { close(inside) })
		<-release
	}, nil))

	require.NoError(cq.Publish(newScopeEvent(scopeEvOrder))) // taken by the dispatcher, which blocks
	<-inside
	require.NoError(cq.Publish(newScopeEvent(scopeEvOrder))) // fills the single buffer slot

	third := make(chan error, 1)
	go func() { third <- cq.Publish(newScopeEvent(scopeEvOrder)) }()
	select {
	case <-third:
		t.Fatal("third Publish returned: the buffer is larger than requested")
	case <-time.After(100 * time.Millisecond):
	}

	close(release)
	require.NoError(<-third)
	require.NoError(cq.Stop())
}

// Defaults: one partition, default buffer size, empty name.
func TestBusConfigDefaults(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	cfg := newBusConfig()
	require.Equal(1, cfg.Partitions)
	require.Equal(DefaultBusBufferSize, cfg.BufferSize)
	require.Empty(cfg.Name)
}
