package goflows

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
)

// InMemoryTransportName is what InMemoryTransport.Name returns.
const InMemoryTransportName = "inmemory"

// inMemoryBus is a bounded queue of events. The channel is never closed:
// publishers select on it together with done and the caller context, so a
// Publish racing with Stop returns ErrBusClosed instead of panicking on a
// closed channel, and a Publish on a full bus gives up when ctx is done.
type inMemoryBus struct {
	btype  EventBus
	ch     chan Event
	done   chan struct{}
	closed atomic.Bool
}

func newInMemoryBus(btype EventBus, size int) *inMemoryBus {
	return &inMemoryBus{
		btype: btype,
		ch:    make(chan Event, size),
		done:  make(chan struct{}),
	}
}

func (b *inMemoryBus) publish(ctx context.Context, ev Event) error {
	if b.closed.Load() {
		return ErrBusClosed
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	select {
	case b.ch <- ev:
		return nil
	case <-b.done:
		return ErrBusClosed
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *inMemoryBus) close() {
	if b.closed.CompareAndSwap(false, true) {
		close(b.done)
	}
}

// InMemoryTransport is a channel based [Transport] for a single process. The
// zero value is ready to use; it implements [LoggerSetter] so the engine hands
// it its logger.
type InMemoryTransport struct {
	logger atomic.Pointer[slog.Logger]
	inputs sync.Map
}

func (eh *InMemoryTransport) Name() string {
	return InMemoryTransportName
}

// SetLogger sets the logger; nil is ignored.
func (eh *InMemoryTransport) SetLogger(l *slog.Logger) {
	if l != nil {
		eh.logger.Store(l.With("transport", InMemoryTransportName))
	}
}

// log returns the current logger, or a silent one if none was set.
func (eh *InMemoryTransport) log() *slog.Logger {
	if l := eh.logger.Load(); l != nil {
		return l
	}
	return slog.New(slog.DiscardHandler)
}

func (eh *InMemoryTransport) Open(btype EventBus, cfg BusConfig) error {
	if eh.Has(btype) {
		return ErrBusExists
	}

	if cfg.BufferSize < 1 {
		return ErrOptionInvalid
	}

	bus := newInMemoryBus(btype, cfg.BufferSize)

	eh.log().Debug("registering event bus", "bus-type", btype, "bus-name", cfg.Name, "buffer-size", cfg.BufferSize)

	if _, loaded := eh.inputs.LoadOrStore(btype, bus); loaded {
		return ErrBusExists
	}

	return nil
}

func (eh *InMemoryTransport) Has(btype EventBus) bool {
	_, ok := eh.inputs.Load(btype)
	return ok
}

// Stream returns the channel events for btype are delivered on. The channel
// is never closed; the engine stops reading from it after Close.
func (eh *InMemoryTransport) Stream(btype EventBus) (<-chan Event, bool) {
	abus, ok := eh.inputs.Load(btype)
	if !ok {
		return nil, ok
	}

	bus := abus.(*inMemoryBus)
	return bus.ch, ok
}

func (eh *InMemoryTransport) Publish(ctx context.Context, btype EventBus, ev Event) error {
	abus, ok := eh.inputs.Load(btype)
	if !ok {
		return ErrBusNotFound
	}

	return abus.(*inMemoryBus).publish(ctx, ev)
}

// Close marks every bus closed and releases publishers blocked on a full
// queue. Events already queued stay in the channel for the engine to drain.
func (eh *InMemoryTransport) Close() error {
	eh.log().Debug("closing transport")

	eh.inputs.Range(func(k, v interface{}) bool {
		v.(*inMemoryBus).close()
		return true
	})

	return nil
}
