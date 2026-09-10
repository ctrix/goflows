package goflows

import (
	"log/slog"
	"os"
	"strings"
	"sync"
)

const (
	INMEMORY_HANDLER_NAME       = "InMemoryEventHandler"
	MAX_INMEMORY_QUEUE_ELEMENTS = 111
)

type inMemoryBus struct {
	btype EventBus
	wgIn  sync.WaitGroup
	in    chan EventInterface
	out   chan EventInterface
}

func NewInMemoryBus(btype EventBus) *inMemoryBus {
	return &inMemoryBus{
		btype: btype,
		in:    make(chan EventInterface, MAX_INMEMORY_QUEUE_ELEMENTS),
		out:   make(chan EventInterface, MAX_INMEMORY_QUEUE_ELEMENTS),
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
		// case "name":
		// 	if val, ok := opt.Value.(string); ok {
		// 		bus.name = eh.sanitizeInMemoryBusName(val)
		// 	}
		default:
			eh.logger.Error("cannot handle unknown option while registering event bus", "type", btype, "optname", opt.Name)
		}
	}

	eh.logger.Debug("registering event bus", "type", btype)

	if _, ok := eh.inputs.Load(btype); ok {
		return EEventBusExists
	}

	eh.inputs.Store(btype, bus)

	bus.wgIn.Add(1)
	go func() {
		defer bus.wgIn.Done()
		for {
			select {
			case ev, ok := <-bus.in:
				if !ok {
					return
				}
				bus.out <- ev
			}
		}
	}()

	return nil
}

func (eh *InMemoryEventHandler) BusExists(btype EventBus) bool {
	_, ok := eh.inputs.Load(btype)
	return ok
}

func (eh *InMemoryEventHandler) Range(btype EventBus) (<-chan EventInterface, bool) {
	abus, ok := eh.inputs.Load(btype)
	if !ok {
		return nil, ok
	}

	bus := abus.(*inMemoryBus)
	return bus.out, ok
}

func (eh *InMemoryEventHandler) Publish(btype EventBus, ev EventInterface) error {
	abus, ok := eh.inputs.Load(btype)
	if !ok {
		return EEventBusDoesntExists
	}

	bus := abus.(*inMemoryBus)
	bus.in <- ev
	// eh.logger.Debug("published event handler", "id", ev.GetID(), "type", ev.GetType())

	return nil
}

func (eh *InMemoryEventHandler) Stop() {
	eh.logger.Debug("stopping event handler")

	eh.inputs.Range(func(k, v interface{}) bool {
		bus := v.(*inMemoryBus)
		close(bus.in)
		bus.wgIn.Wait()
		close(bus.out)
		return true
	})
}
