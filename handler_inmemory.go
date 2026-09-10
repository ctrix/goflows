package goflows

import (
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
)

const (
	INMEMORY_HANDLER_NAME       = "InMemoryEventHandler"
	MAX_INMEMORY_QUEUE_ELEMENTS = 111
)

// inMemoryBus is a bounded queue of events. The channel is never closed:
// publishers select on it together with done, so a Publish racing with Stop
// returns EEventBusClosed instead of panicking on a closed channel.
type inMemoryBus struct {
	btype  EventBus
	ch     chan EventInterface
	done   chan struct{}
	closed atomic.Bool
}

func NewInMemoryBus(btype EventBus) *inMemoryBus {
	return &inMemoryBus{
		btype: btype,
		ch:    make(chan EventInterface, MAX_INMEMORY_QUEUE_ELEMENTS),
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

type InMemoryEventHandler struct {
	logger *slog.Logger
	inputs sync.Map
}

func (eh *InMemoryEventHandler) Initialize(opts ...Option) EventHandlerInterface {
	eh.logger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	return eh
}

func (eh *InMemoryEventHandler) Name() string {
	return INMEMORY_HANDLER_NAME
}

func (eh *InMemoryEventHandler) SetLogger(l *slog.Logger) {
	if l != nil {
		eh.logger = l.With("handler", INMEMORY_HANDLER_NAME)
	}
}

func (eh *InMemoryEventHandler) sanitizeInMemoryBusName(n string) string {
	n = strings.ToLower(n)
	n = nonAlphanumericRegex.ReplaceAllString(n, "_")
	return n
}

func (eh *InMemoryEventHandler) RegisterBus(btype EventBus, opts ...*Option) error {
	if eh.BusExists(btype) {
		return EEventBusExists
	}

	bus := NewInMemoryBus(btype)

	for _, opt := range opts {
		switch opt.Name {
		case "name", "partitions":
			// Handled by the engine, nothing to do here.
		default:
			eh.logger.Debug("ignoring unknown option while registering event bus", "type", btype, "optname", opt.Name)
		}
	}

	eh.logger.Debug("registering event bus", "type", btype)

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
	eh.logger.Debug("stopping event handler")

	eh.inputs.Range(func(k, v interface{}) bool {
		v.(*inMemoryBus).close()
		return true
	})
}
