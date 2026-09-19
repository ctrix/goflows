package goflows_test

import (
	"context"
	"fmt"

	"github.com/ctrix/goflows"
)

const busMain goflows.EventBus = iota + 1

const eventUserCreated goflows.EventType = iota + 1

type userCreated struct {
	goflows.BaseEvent
	UserID string
}

// The README example, kept compiling and behaving as documented.
func Example() {
	cq, err := goflows.NewEngine(new(goflows.InMemoryTransport))
	if err != nil {
		panic(err)
	}

	if err := cq.RegisterBus(busMain, goflows.WithBusName("main")); err != nil {
		panic(err)
	}
	if err := cq.RegisterEvent(busMain, eventUserCreated,
		goflows.WithEventName("user created"), goflows.OfType[*userCreated]()); err != nil {
		panic(err)
	}
	if err := cq.Start(); err != nil {
		panic(err)
	}

	sub, err := goflows.Subscribe(cq, busMain, eventUserCreated, func(ev *userCreated) {
		fmt.Println("user created:", ev.UserID)
	})
	if err != nil {
		panic(err)
	}
	defer sub.Unsubscribe()

	ev := &userCreated{BaseEvent: goflows.NewBaseEvent(eventUserCreated), UserID: "42"}
	if err := cq.Publish(context.Background(), ev); err != nil {
		panic(err)
	}

	// Stop drains the bus, so the subscriber has run by the time it returns.
	if err := cq.Stop(); err != nil {
		panic(err)
	}

	// Output: user created: 42
}
