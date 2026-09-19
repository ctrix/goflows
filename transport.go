package goflows

import (
	"context"
	"log/slog"
)

// Transport moves events from Publish to the dispatcher of each bus. The
// in-memory implementation is [InMemoryTransport]; a broker backed one
// implements the same interface.
type Transport interface {
	// Name identifies the transport in logs.
	Name() string
	// Open creates the bus. It fails with ErrBusExists if it is already
	// open.
	Open(bus EventBus, cfg BusConfig) error
	// Has reports whether the bus is open.
	Has(bus EventBus) bool
	// Stream returns the channel the engine reads events for bus from. The
	// channel may stay open after Close: the engine stops reading on its own.
	Stream(bus EventBus) (<-chan Event, bool)
	// Publish enqueues ev on bus. It may block while the bus is full and must
	// return ctx.Err() once ctx is done, and ErrBusClosed after Close.
	Publish(ctx context.Context, bus EventBus, ev Event) error
	// Close releases publishers blocked on a full bus with ErrBusClosed
	// and stops accepting events. Events already queued must stay readable
	// from Stream so the engine can drain them.
	Close() error
}

// LoggerSetter is implemented by transports that want the engine logger. The
// engine calls it once at construction; it is not part of Transport.
type LoggerSetter interface {
	SetLogger(l *slog.Logger)
}
