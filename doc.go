// Package goflows provides a small event bus engine for Go applications built
// around the CQRS / event-driven pattern.
//
// The engine ([CQRS]) routes typed events ([EventInterface]) over named buses
// ([EventBus]) through a pluggable transport ([EventHandlerInterface]).
// An in-memory transport ([InMemoryEventHandler]) is provided out of the box.
//
// Typical usage:
//
//	cq, err := goflows.NewCQRSEngine(new(goflows.InMemoryEventHandler))
//	// handle err
//
//	// The library is silent by default. Pass a logger to see what it does:
//	// goflows.NewCQRSEngine(handler, goflows.WithLogger(slog.Default()))
//
//	cq.RegisterBus(MyBus, goflows.WithBusName("main"), goflows.WithPartitions(2))
//	cq.RegisterEvent(MyBus, MyEventType, goflows.WithEventName("user created"))
//	cq.Start()
//	defer cq.Stop()
//
//	sub, err := cq.Subscribe(MyBus, MyEventType, func(ev goflows.EventInterface) {
//		// react to the event
//	})
//	// handle err; later: sub.Unsubscribe()
//
//	cq.Publish(&MyEvent{})
//
// # Delivery semantics
//
// Every bus is consumed by one dispatcher with a configurable number of
// partitions, set with [WithPartitions] on RegisterBus (default 1). The queue
// length of the bus is set with [WithBufferSize] (default
// [DefaultBusBufferSize]).
//
//   - With one partition, events are delivered sequentially in publish order.
//     Subscribers run one after the other in the dispatcher goroutine, so a
//     slow subscriber delays every later event on that bus.
//   - With N partitions, up to N events are delivered concurrently and the
//     order across events is not guaranteed. A single event is still handed
//     to its subscribers one after the other.
//
// A subscriber that panics is logged and skipped; the bus keeps running.
//
// The in-memory transport is a bounded queue: Publish blocks while the bus is
// full. A subscriber that publishes synchronously on the bus it is consuming
// can therefore deadlock once that bus is full. Publish replies on a different
// bus, or publish asynchronously from inside a subscriber.
//
// Stop releases publishers blocked on a full bus with an error, then delivers
// every event already queued before returning.
package goflows
