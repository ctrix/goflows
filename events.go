package goflows

import (
	"time"

	"github.com/google/uuid"
)

type EventBus uint32
type EventType uint32

// Event is what the engine routes. Embed [BaseEvent] in your own struct to
// satisfy it.
type Event interface {
	GetID() string
	GetName() string
	GetType() EventType
	// GetReferrer returns the ID of the event this one replies to, or "".
	GetReferrer() string
}

// BaseEvent carries the fields every event needs. Build it with
// [NewBaseEvent] and embed it:
//
//	type UserCreated struct {
//		goflows.BaseEvent
//		UserID string
//	}
//	ev := &UserCreated{BaseEvent: goflows.NewBaseEvent(EventUserCreated), UserID: "42"}
//
// The zero value has an invalid type and is rejected by Publish. The accessors
// keep the Get prefix because the fields they read share their names and Go
// does not allow a field and a method with the same name.
type BaseEvent struct {
	ID       string    `json:"id"`
	Type     EventType `json:"type"`
	Time     time.Time `json:"ts"`
	Referrer string    `json:"referrer,omitempty"`
	Name     string    `json:"name"`
}

// NewBaseEvent returns a BaseEvent of the given type with a fresh UUID v7 ID,
// time ordered, and the current time.
func NewBaseEvent(typ EventType) BaseEvent {
	return BaseEvent{
		ID:   uuid.Must(uuid.NewV7()).String(),
		Type: typ,
		Time: time.Now(),
	}
}

func (e BaseEvent) GetID() string { return e.ID }

func (e BaseEvent) GetName() string { return e.Name }

func (e BaseEvent) GetType() EventType { return e.Type }

func (e BaseEvent) GetReferrer() string { return e.Referrer }

// EventSubscriptionCallback is invoked for every event delivered to a
// subscription. Capture any state you need in the closure.
type EventSubscriptionCallback func(ev Event)
