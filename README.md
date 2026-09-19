# goflows

A lightweight event bus for Go with typed events, pluggable transports and
request/response messaging, built for CQRS-style applications.

- Typed events routed over named buses; `OfType` binds an event type value to
  one Go type so colliding constants fail instead of cross-delivering
- Generic `Subscribe[T]` and `Request[T]`: callbacks and replies already typed,
  `goflows.Event` as `T` for the untyped form
- Pluggable transport (`Transport`), with an in-memory
  implementation included
- Subscribe returns a handle; `Unsubscribe` on it removes exactly that subscription
- `Request[T]` for request/response style flows, bounded by a context
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
	"context"
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
	goflows.BaseEvent
	UserID string
}

func main() {
	cq, err := goflows.NewEngine(
		new(goflows.InMemoryTransport),
		goflows.WithLogger(slog.Default()),
	)
	if err != nil {
		panic(err)
	}

	if err := cq.RegisterBus(BusMain, goflows.WithBusName("main")); err != nil {
		panic(err)
	}
	if err := cq.RegisterEvent(BusMain, EventUserCreated,
		goflows.WithEventName("user created"), goflows.OfType[*UserCreated]()); err != nil {
		panic(err)
	}

	if err := cq.Start(); err != nil {
		panic(err)
	}
	defer cq.Stop()

	sub, err := goflows.Subscribe(cq, BusMain, EventUserCreated, func(ev *UserCreated) {
		slog.Info("got event", "id", ev.GetID(), "user", ev.UserID)
	})
	if err != nil {
		panic(err)
	}
	defer sub.Unsubscribe()

	ev := &UserCreated{BaseEvent: goflows.NewBaseEvent(EventUserCreated), UserID: "42"}
	ev.Name = "user created"
	_ = cq.Publish(context.Background(), ev)
}
```

## Concepts

| Type | Role |
| --- | --- |
| `Engine` | The engine, built with `NewEngine`. Owns buses, event registrations and subscriptions. |
| `EventBus` | Identifier of a bus. Buses are registered with `RegisterBus`. |
| `EventType` | Identifier of an event type. Bound to a bus with `RegisterEvent`. |
| `Event` / `BaseEvent` | Event contract and the base struct to embed in your own events, built with `NewBaseEvent`. |
| `Subscribe[T]` | Package function: `goflows.Subscribe(cq, bus, type, func(ev *T))`. Use `goflows.Event` as `T` to receive everything untyped. |
| `Request[T]` | Package function: publish and wait for the reply as `T`. |
| `Subscription` | Handle returned by `Subscribe`, with `Unsubscribe`, `BindContext`, `Bus` and `Type`. |
| `Transport` | Transport abstraction: `Open`, `Has`, `Stream`, `Publish`, `Close`. Implement it to back the engine with a broker; add `SetLogger` to receive the engine logger. |
| `InMemoryTransport` | Built-in channel-based transport, suitable for single-process use. |
| `BusOption` | Functional options for `RegisterBus`: `WithBusName`, `WithPartitions`, `WithBufferSize`. |
| `EventOption` | Functional options for `RegisterEvent`: `WithEventName`, `OfType[T]`. |
| `EngineOption` | Functional options for `NewEngine`: `WithLogger`. |

## Delivery semantics

- One dispatcher per bus, with `WithPartitions(n)` worker goroutines
  (default 1). The bus queue length is `WithBufferSize(n)` (default 111).
- One partition: events are delivered in publish order, subscribers run one
  after the other. A slow subscriber delays later events on that bus.
- N partitions: up to N events in flight, no ordering across events. Each
  event still reaches its subscribers sequentially.
- A subscriber that panics is logged and skipped. The bus keeps running.
- Whether `Publish` blocks is a property of the transport, not of the engine:
  the engine keeps no queue of its own and adds no waiting. The in-memory
  transport is a bounded queue, so `Publish(ctx, ev)` blocks while the bus is
  full until `ctx` is done, then returns `ctx.Err()`. A broker transport with
  an asynchronous producer returns at once. A subscriber
  publishing synchronously on its own bus stalls the dispatcher while the bus
  is full: give that `Publish` a deadline, use a different bus for replies, or
  publish asynchronously.
- `Request(ctx, bus, ev, replyType)` publishes and waits for a reply whose
  `Referrer` is the request ID. A timeout is `context.DeadlineExceeded`, an
  external cancel is `context.Canceled`.
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
