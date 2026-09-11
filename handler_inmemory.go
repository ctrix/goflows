package goflows

import (
	"log/slog"
	"sync"
	"sync/atomic"
)

const INMEMORY_HANDLER_NAME = "InMemoryEventHandler"

// inMemoryBus is a bounded queue of events. The channel is never closed:
// publishers select on it together with done, so a Publish racing with Stop
// returns EEventBusClosed instead of panicking on a closed channel.
type inMemoryBus struct {
	btype  EventBus
	ch     chan EventInterface
	done   chan struct{}
	closed atomic.Bool
}

func newInMemoryBus(btype EventBus, size int) *inMemoryBus {
	return &inMemoryBus{
		btype: btype,
		ch:    make(chan EventInterface, size),
		done:  make(chan struct{}),
	}
}

func (b *inMemoryBus) publish(ev EventInterface) error {
	if b.closed.Load() {
		return EEventBusClosed
	}

	select {
	case b.ch <- ev:
		return nil
	case <-b.done:
		return EEventBusClosed
	}
}

func (b *inMemoryBus) close() {
	if b.closed.CompareAndSwap(false, true) {
		close(b.done)
	}
}

// InMemoryEventHandler is a channel based transport for a single process.
// The zero value is ready to use; the engine hands it its logger.
type InMemoryEventHandler struct {
	logger atomic.Pointer[slog.Logger]
	inputs sync.Map
}

func (eh *InMemoryEventHandler) Name() string {
	return INMEMORY_HANDLER_NAME
}

// SetLogger sets the logger; nil is ignored.
func (eh *InMemoryEventHandler) SetLogger(l *slog.Logger) {
	if l != nil {
		eh.logger.Store(l.With("handler", INMEMORY_HANDLER_NAME))
	}
}

// log returns the current logger, or a silent one if none was set.
func (eh *InMemoryEventHandler) log() *slog.Logger {
	if l := eh.logger.Load(); l != nil {
		return l
	}
	return slog.New(slog.DiscardHandler)
}

func (eh *InMemoryEventHandler) RegisterBus(btype EventBus, cfg BusConfig) error {
	if eh.BusExists(btype) {
		return EEventBusExists
	}

	if cfg.BufferSize < 1 {
		return EOptionInvalid
	}

	bus := newInMemoryBus(btype, cfg.BufferSize)

	eh.log().Debug("registering event bus", "bus-type", btype, "bus-name", cfg.Name, "buffer-size", cfg.BufferSize)

	if _, loaded := eh.inputs.LoadOrStore(btype, bus); loaded {
		return EEventBusExists
	}

	return nil
}

func (eh *InMemoryEventHandler) BusExists(btype EventBus) bool {
	_, ok := eh.inputs.Load(btype)
	return ok
}

// Range returns the channel events for btype are delivered on. The channel is
// never closed; the engine stops reading from it after Stop.
func (eh *InMemoryEventHandler) Range(btype EventBus) (<-chan EventInterface, bool) {
	abus, ok := eh.inputs.Load(btype)
	if !ok {
		return nil, ok
	}

	bus := abus.(*inMemoryBus)
	return bus.ch, ok
}

func (eh *InMemoryEventHandler) Publish(btype EventBus, ev EventInterface) error {
	abus, ok := eh.inputs.Load(btype)
	if !ok {
		return EEventBusDoesntExists
	}

	return abus.(*inMemoryBus).publish(ev)
}

// Stop marks every bus closed and releases publishers blocked on a full
// queue. Events already queued stay in the channel for the engine to drain.
func (eh *InMemoryEventHandler) Stop() {
	eh.log().Debug("stopping event handler")

	eh.inputs.Range(func(k, v interface{}) bool {
		v.(*inMemoryBus).close()
		return true
	})
}
