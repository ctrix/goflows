package goflows

import (
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
	Publish(EventBus, EventInterface) error
	Stop()
}
