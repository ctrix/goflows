package goflows

import (
	"time"

	"github.com/google/uuid"
)

type EventBus uint32
type EventType uint32

type Event struct {
	ID       string    `json:"id"`
	Type     EventType `json:"type"`
	TS       int64     `json:"ts"`
	Referrer string    `json:"referrer,omitempty"`
	Name     string    `json:"name"`
}

type EventInterface interface {
	Init()
	GetID() string
	GetName() string
	GetType() EventType
	GetReferrer() *string
}

// BaseInit assigns a fresh ID and timestamp. It deliberately leaves Type at
// EventTypeInvalid: an event whose type was never set must be rejected by
// Publish, not silently routed to whatever type happens to be registered as 1.
func (e *Event) BaseInit() {
	e.ID = uuid.Must(uuid.NewUUID()).String()
	e.TS = time.Now().Unix()
}

func (e *Event) GetID() string {
	return e.ID
}

func (e *Event) GetName() string {
	return e.Name
}

func (e *Event) GetType() EventType {
	return e.Type
}

func (e *Event) GetReferrer() *string {
	if e.Referrer != "" {
		return &e.Referrer
	}
	return nil
}

// EventSubscriptionCallback is invoked for every event delivered to a
// subscription. Capture any state you need in the closure.
type EventSubscriptionCallback func(ev EventInterface)
