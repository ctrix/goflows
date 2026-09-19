package goflows

import (
	"context"
	"log/slog"
)

type EventHandlerInterface interface {
	Name() string
	// SetLogger is called by the engine at construction with the engine
	// logger. Implementations must accept nil by ignoring it.
	SetLogger(l *slog.Logger)
	RegisterBus(btype EventBus, cfg BusConfig) error
	BusExists(btype EventBus) bool
	Range(btype EventBus) (<-chan EventInterface, bool)
	// Publish enqueues ev on btype. It may block while the bus is full and
	// must return ctx.Err() once ctx is done.
	Publish(ctx context.Context, btype EventBus, ev EventInterface) error
	Stop()
}
