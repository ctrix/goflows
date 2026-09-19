// Package goflows provides a small event bus engine for Go applications built
// around the CQRS / event-driven pattern.
//
// The engine ([CQRS]) routes typed events ([Event]) over named buses
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
//	sub, err := cq.Subscribe(MyBus, MyEventType, func(ev goflows.Event) {
//		// react to the event
//	})
//	// handle err; later: sub.Unsubscribe()
//
//	cq.Publish(ctx, &MyEvent{BaseEvent: goflows.NewBaseEvent(MyEventType)})
//
//	// Request/reply: publish and wait for an event of ReplyType on MyBus whose
//	// Referrer is the request ID. ctx bounds the wait.
//	reply, err := cq.Request(ctx, MyBus, &MyRequest{}, ReplyType)
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
// full, until ctx is done, and then returns ctx.Err(). A subscriber that
// publishes synchronously on the bus it is consuming can therefore stall the
// dispatcher while that bus is full: give such a Publish a deadline, publish
// replies on a different bus, or publish asynchronously.
//
// Stop releases publishers blocked on a full bus with an error, then delivers
// every event already queued before returning.
package goflows
