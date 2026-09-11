package goflows

import (
	"log/slog"
)

type EventHandlerInterface interface {
	Name() string
	// Initialize(*slog.Logger) EventHandlerInterface
	SetLogger(l *slog.Logger)
	RegisterBus(btype EventBus, cfg BusConfig) error
	BusExists(btype EventBus) bool
	Range(btype EventBus) (<-chan EventInterface, bool)
	Publish(EventBus, EventInterface) error
	Stop()
}
