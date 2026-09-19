# goflows

A lightweight event bus for Go with typed events, pluggable transports and
request/response messaging, built for CQRS-style applications.

- Typed events routed over named buses
- Pluggable transport (`EventHandlerInterface`), with an in-memory
  implementation included
- Subscribe returns a handle; `Unsubscribe` on it removes exactly that subscription
- `PublishAndWait` for request/response style flows
- Silent by default; structured logging via `log/slog` with `WithLogger`

## Install

```sh
go get github.com/ctrix/goflows
```

Requires Go 1.26 or newer.

## Usage

```go
package main

import (
	"log/slog"

	"github.com/ctrix/goflows"
)

const (
	BusMain goflows.EventBus = iota + 1
)

const (
	EventUserCreated goflows.EventType = iota + 1
)

type UserCreated struct {
	goflows.Event
	UserID string
}

func (e *UserCreated) Init() {
	e.BaseInit()
	e.Name = "user created"
	e.Type = EventUserCreated
}

func main() {
	cq, err := goflows.NewCQRSEngine(
		new(goflows.InMemoryEventHandler),
		goflows.WithLogger(slog.Default()),
	)
	if err != nil {
		panic(err)
	}

	if err := cq.RegisterBus(BusMain, goflows.WithBusName("main")); err != nil {
		panic(err)
	}
	if err := cq.RegisterEvent(BusMain, EventUserCreated, goflows.WithEventName("user created")); err != nil {
		panic(err)
	}

	if err := cq.Start(); err != nil {
		panic(err)
	}
	defer cq.Stop()

	sub, err := cq.Subscribe(BusMain, EventUserCreated, func(ev goflows.EventInterface) {
		slog.Info("got event", "id", ev.GetID(), "name", ev.GetName())
	})
	if err != nil {
		panic(err)
	}
	defer sub.Unsubscribe()

	ev := &UserCreated{UserID: "42"}
	ev.Init()
	_ = cq.Publish(ev)
}
```

## Concepts

| Type | Role |
| --- | --- |
| `CQRS` | The engine. Owns buses, event registrations and subscriptions. |
| `EventBus` | Identifier of a bus. Buses are registered with `RegisterBus`. |
| `EventType` | Identifier of an event type. Bound to a bus with `RegisterEvent`. |
| `EventInterface` / `Event` | Event contract and a base struct to embed in your own events. |
| `Subscription` | Handle returned by `Subscribe`, with `Unsubscribe`, `Bus` and `Type`. |
| `EventHandlerInterface` | Transport abstraction. Implement it to back the engine with another broker. |
| `InMemoryEventHandler` | Built-in channel-based transport, suitable for single-process use. |
| `BusOption` | Functional options for `RegisterBus`: `WithBusName`, `WithPartitions`, `WithBufferSize`. |
| `EventOption` | Functional options for `RegisterEvent`: `WithEventName`. |
| `EngineOption` | Functional options for `NewCQRSEngine`: `WithLogger`. |

## Delivery semantics

- One dispatcher per bus, with `WithPartitions(n)` worker goroutines
  (default 1). The bus queue length is `WithBufferSize(n)` (default 111).
- One partition: events are delivered in publish order, subscribers run one
  after the other. A slow subscriber delays later events on that bus.
- N partitions: up to N events in flight, no ordering across events. Each
  event still reaches its subscribers sequentially.
- A subscriber that panics is logged and skipped. The bus keeps running.
- The in-memory transport is a bounded queue. `Publish` blocks while the bus
  is full, so a subscriber publishing synchronously on its own bus can
  deadlock under load: use a different bus for replies, or publish
  asynchronously.
- `Stop` releases blocked publishers with an error and drains what is already
  queued before returning.

## Development

```sh
make test        # run tests
make test-race   # run tests with the race detector
make vet         # go vet
make cover       # coverage report in coverage.html
```

## License

MIT. See [LICENSE](LICENSE).
