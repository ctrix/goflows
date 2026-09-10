package goflows

import (
	"context"
	"sync"
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

func (e *Event) BaseInit() {
	e.ID = uuid.Must(uuid.NewUUID()).String()
	e.TS = time.Now().Unix()
	e.Type = 1
	return
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

type EventSubscriptionCallback func(ev EventInterface, cbdata any)

type EventSubscriptionObject struct {
	btype  EventBus
	etype  EventType
	cb     EventSubscriptionCallback
	cbdata any
	cond   *sync.Cond
	ctx    context.Context
	cancel context.CancelFunc
}
