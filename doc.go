// Package goflows provides a small event bus engine for Go applications built
// around the CQRS / event-driven pattern.
//
// The engine ([CQRS]) routes typed events ([EventInterface]) over named buses
// ([EventBus]) through a pluggable transport ([EventHandlerInterface]).
// An in-memory transport ([InMemoryEventHandler]) is provided out of the box.
//
// Typical usage:
//
//	eh := new(goflows.InMemoryEventHandler)
//	eh.Initialize()
//
//	cq, err := goflows.NewCQRSEngine(eh)
//	// handle err
//
//	cq.RegisterBus(MyBus, goflows.NewOption("name", "main"))
//	cq.RegisterEvent(MyBus, MyEventType, goflows.NewOption("name", "user created"))
//	cq.Start()
//	defer cq.Stop()
//
//	cq.Subscribe(MyBus, MyEventType, func(ev goflows.EventInterface, cbdata any) {
//		// react to the event
//	}, nil)
//
//	cq.Publish(&MyEvent{})
package goflows
