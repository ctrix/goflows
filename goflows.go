package goflows

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"
)

const (
	LIBRARY_NAME     = "CQRS"
	EventBusInvalid  = 0
	EventTypeInvalid = 0
)

var nonAlphanumericRegex = regexp.MustCompile(`[^a-zA-Z0-9_]+`)

var (
	EEventTypeExists               = errors.New("event type already exists")
	EEventTypeInvalid              = errors.New("event type is invalid or unknown")
	EEventRegistrationExists       = errors.New("event registration already exists")
	EEventRegistrationDoesntExists = errors.New("event registration doesnt exists")
	EEventTypeDoesntExists         = errors.New("event type does not exist")
	EEventBusExists                = errors.New("event bus already exists")
	EEventBusDoesntExists          = errors.New("event bus does not exists")
	EEventBusInvalid               = errors.New("event bus is invalid")
	EEventNotRegisteredOnBus       = errors.New("event type is not registered on this bus")
	EEventHandlerNull              = errors.New("event handler is null")
	EEventHandlerRedefined         = errors.New("event handler is already set and cannot be redefined")
	EEventHandlerInvalid           = errors.New("event handler is invalid")
	EEventBusNotFound              = errors.New("corresponding event bus not found")
	EEngineStarted                 = errors.New("engine already started")
	EEngineStopped                 = errors.New("engine is stopped")
	ESubscriptionInvalid           = errors.New("subscription request is invalid")
	EUnsubscriptionInvalid         = errors.New("unsubscription request is invalid")
)

// subscriptionKey identifies the set of subscribers listening for a given
// event type on a given bus. Subscriptions are scoped to the bus: a subscriber
// on bus A never sees copies of the same event delivered on bus B.
type subscriptionKey struct {
	btype EventBus
	etype EventType
}

type BusDispatcher struct {
	btype EventBus
	ch    <-chan EventInterface
	wg    sync.WaitGroup
}

type Option struct {
	Name  string
	Value any
}

func NewOption(name string, value any) *Option {
	return &Option{
		Name:  name,
		Value: value,
	}
}

// engineState is the lifecycle of a CQRS engine: created -> started -> stopped.
// Registration and subscription work in both created and started; nothing
// works once stopped.
type engineState int32

const (
	engineCreated engineState = iota
	engineStarted
	engineStopped
)

type CQRS struct {
	logger *slog.Logger

	handler    EventHandlerInterface
	eventTypes sync.Map // this is a map[EventType][]EventBus

	stateMu sync.RWMutex
	state   engineState

	// subMu guards subscriptions. The slices stored as values are treated as
	// immutable once published: readers copy the slice header under RLock and
	// iterate without the lock, writers replace the slice under Lock.
	subMu         sync.RWMutex
	subscriptions map[subscriptionKey][]*EventSubscriptionObject

	subDispatchers sync.Map // this is a map[btype]*BusDispatcher
}

func NewCQRSEngine(eventh EventHandlerInterface) (*CQRS, error) {
	if eventh == nil {
		return nil, EEventHandlerInvalid
	}

	c := &CQRS{
		handler:       eventh,
		subscriptions: make(map[subscriptionKey][]*EventSubscriptionObject),
	}

	c.SetLogger(nil)

	return c, nil
}

func (c *CQRS) GetLogger() *slog.Logger {
	return c.logger
}

func (c *CQRS) SetLogger(l *slog.Logger) error {
	// TODO
	if l == nil {
		l = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
		// slog.SetDefault(l)
	}

	c.logger = l.With("library", LIBRARY_NAME)

	return nil
}

// checkNotStopped returns EEngineStopped once Stop has been called.
func (c *CQRS) checkNotStopped() error {
	c.stateMu.RLock()
	defer c.stateMu.RUnlock()

	if c.state == engineStopped {
		return EEngineStopped
	}

	return nil
}

func (c *CQRS) busDispatcherRun(btype EventBus) error {
	ch, ok := c.handler.Range(btype)
	if !ok {
		return nil // TODO We should return a specific error, even if this is unlikely to happen
	}

	dis := &BusDispatcher{
		btype: btype,
		ch:    ch,
	}

	// Register the dispatcher before starting its goroutine, so that a Stop()
	// racing with RegisterBus() always finds it and waits for it.
	c.subDispatchers.Store(dis.btype, dis)

	dis.wg.Add(1)
	go func(d *BusDispatcher) {
		warned := false
		defer d.wg.Done()

		for {
			select {
			case ev, ok := <-d.ch:
				if ok {
					subs := c.subscribersFor(d.btype, ev.GetType())
					if len(subs) == 0 {
						// There are no subscriptions.
						if !warned {
							// warned = true
							c.logger.Warn("no subscriptions found for event on bus", "bus-type", d.btype, "event-type", ev.GetType())
						}
					} else {
						for _, sub := range subs {
							sub.cb(ev, sub.cbdata)
						}
					}
				} else {
					// Channel was closed, the bus is no more active
					return
				}
			}
		}
	}(dis)

	return nil
}

func (c *CQRS) RegisterBus(btype EventBus, opts ...*Option) error {
	if err := c.checkNotStopped(); err != nil {
		return err
	}

	if btype == EventBusInvalid {
		return EEventBusInvalid
	}

	if c.handler == nil {
		return EEventHandlerNull
	}

	err := c.handler.RegisterBus(btype, opts...)
	if err == nil {
		// Start the bus subscription handler
		c.busDispatcherRun(btype)
	}

	return err
}

func (c *CQRS) sanitizeEventName(n string) string {
	n = strings.ToLower(n)
	n = nonAlphanumericRegex.ReplaceAllString(n, "_")
	return n
}

func (c *CQRS) RegisterEvent(btype EventBus, etype EventType, opts ...*Option) error {
	if err := c.checkNotStopped(); err != nil {
		return err
	}

	if etype == EventTypeInvalid {
		return EEventTypeInvalid
	}

	if !c.handler.BusExists(btype) {
		return EEventBusDoesntExists
	}

	// if _, ok := c.eventTypes.Load(etype); ok {
	//	return EEventTypeExists
	// }

	var name string
	for _, opt := range opts {
		switch opt.Name {
		case "name":
			if val, ok := opt.Value.(string); ok {
				name = c.sanitizeEventName(val)
			}
		default:
			c.logger.Debug("cannot handle unknown option while registering event type", "type", etype, "optname", opt.Name)
		}
		// TODO OPTS TO HANDLE (if we really want to do it or need it)
	}

	var sl []EventBus
	var ret error = nil
	if asl, ok := c.eventTypes.Load(etype); !ok {
		sl = []EventBus{btype}
	} else {
		sl = asl.([]EventBus)
		if !inSlice(btype, sl) {
			sl = append(sl, btype)
		} else {
			ret = EEventRegistrationExists
		}
	}

	if ret != nil {
		return ret
	}

	c.eventTypes.Store(etype, sl)
	c.logger.Debug("registering event type", "name", name, "type", etype, "bus", btype)

	return nil
}

func (c *CQRS) Start() error {
	if c.handler == nil {
		return EEventHandlerNull
	}

	c.stateMu.Lock()
	defer c.stateMu.Unlock()

	switch c.state {
	case engineStarted:
		return EEngineStarted
	case engineStopped:
		return EEngineStopped
	}

	c.state = engineStarted
	c.logger.Debug("starting CQRS engine")

	return nil
}

// Stop shuts the transport down and waits for every bus dispatcher to drain
// the events already published. It can be called in any state and is
// idempotent: the second and later calls return nil without doing anything.
func (c *CQRS) Stop() error {
	c.stateMu.Lock()
	if c.state == engineStopped {
		c.stateMu.Unlock()
		return nil
	}
	c.state = engineStopped
	c.stateMu.Unlock()

	c.handler.Stop()

	c.subDispatchers.Range(func(k, v interface{}) bool {
		d := v.(*BusDispatcher)
		d.wg.Wait()
		return true
	})

	c.logger.Debug("CQRS engine stopped")

	return nil
}

// This function is used only in tests
func (c *CQRS) countSubscriptions() int64 {
	c.subMu.RLock()
	defer c.subMu.RUnlock()

	var n int64
	for _, subs := range c.subscriptions {
		n += int64(len(subs))
	}

	return n
}

// subscribersFor returns the current subscriber list for (btype, etype). The
// returned slice is never mutated afterwards, so it is safe to iterate over
// without holding the lock.
func (c *CQRS) subscribersFor(btype EventBus, etype EventType) []*EventSubscriptionObject {
	c.subMu.RLock()
	defer c.subMu.RUnlock()

	return c.subscriptions[subscriptionKey{btype: btype, etype: etype}]
}

// sameSubscription reports whether two subscription requests refer to the same
// callback and callback data.
func sameSubscription(a, b *EventSubscriptionObject) bool {
	return reflect.ValueOf(a.cb).Pointer() == reflect.ValueOf(b.cb).Pointer() && a.cbdata == b.cbdata
}

// checkEventOnBus verifies that btype is a registered bus and that etype has
// been registered on that specific bus.
func (c *CQRS) checkEventOnBus(btype EventBus, etype EventType) error {
	if btype == EventBusInvalid || !c.handler.BusExists(btype) {
		return EEventBusDoesntExists
	}

	buses, ok := c.eventTypes.Load(etype)
	if !ok {
		return EEventTypeDoesntExists
	}

	if !inSlice(btype, buses.([]EventBus)) {
		return EEventNotRegisteredOnBus
	}

	return nil
}

func (c *CQRS) Subscribe(btype EventBus, etype EventType, cb EventSubscriptionCallback, cbdata any) error {
	if etype == EventTypeInvalid {
		return EEventTypeInvalid
	}

	if cb == nil {
		return ESubscriptionInvalid
	}

	if err := c.checkNotStopped(); err != nil {
		return err
	}

	if err := c.checkEventOnBus(btype, etype); err != nil {
		return err
	}

	sub := &EventSubscriptionObject{
		btype:  btype,
		etype:  etype,
		cb:     cb,
		cbdata: cbdata,
	}
	key := subscriptionKey{btype: btype, etype: etype}

	c.subMu.Lock()
	defer c.subMu.Unlock()

	c.logger.Debug("subscribing", "bus-type", btype, "sub-type", etype)

	m := c.subscriptions[key]
	for _, s := range m {
		if sameSubscription(s, sub) {
			// Subscription exists, do nothing
			return nil
		}
	}

	// Slices stored in the map are immutable once published: dispatchers may
	// be iterating over them, so always build a fresh one.
	c.subscriptions[key] = append(slices.Clone(m), sub)

	return nil
}

func (c *CQRS) Unsubscribe(btype EventBus, etype EventType, cb EventSubscriptionCallback, cbdata any) error {
	if etype == EventTypeInvalid {
		return EEventTypeInvalid
	}

	if cb == nil {
		return EUnsubscriptionInvalid
	}

	if err := c.checkNotStopped(); err != nil {
		return err
	}

	if err := c.checkEventOnBus(btype, etype); err != nil {
		return err
	}

	unsub := &EventSubscriptionObject{
		btype:  btype,
		etype:  etype,
		cb:     cb,
		cbdata: cbdata,
	}
	key := subscriptionKey{btype: btype, etype: etype}

	c.subMu.Lock()
	defer c.subMu.Unlock()

	c.logger.Debug("unsubscribing", "bus-type", btype, "unsub-type", etype)

	m := c.subscriptions[key]
	for i, s := range m {
		if sameSubscription(s, unsub) {
			newm := slices.Delete(slices.Clone(m), i, i+1)
			if len(newm) == 0 {
				delete(c.subscriptions, key)
			} else {
				c.subscriptions[key] = newm
			}
			return nil
		}
	}

	// Subscription does not exist, nothing to do
	return nil
}

func (c *CQRS) GetBusTypeFromEventType(etype EventType) ([]EventBus, error) {
	if ereg, ok := c.eventTypes.Load(etype); ok {
		if er, ok := ereg.([]EventBus); ok {
			return er, nil
		} else {
			return nil, EEventRegistrationDoesntExists
		}
	}

	return nil, EEventTypeInvalid
}

func (c *CQRS) GetBusTypeFromEvent(ev EventInterface) ([]EventBus, error) {
	etype := ev.GetType()
	return c.GetBusTypeFromEventType(etype)
}

func (c *CQRS) Publish(ev EventInterface) error {
	if err := c.checkNotStopped(); err != nil {
		return err
	}

	if buslist, err := c.GetBusTypeFromEvent(ev); err != nil {
		return err
	} else {
		for _, bus := range buslist {
			if err := c.handler.Publish(bus, ev); err != nil {
				return err
			}
		}
	}

	return nil
}

func (c *CQRS) PublishAndWait(btype EventBus, ev EventInterface, retet EventType, timeout time.Duration) (EventInterface, error) {
	var retev EventInterface

	ctx, cancel := context.WithTimeout(context.Background(), timeout)

	waitid := ev.GetID()
	cbf := func(cur EventInterface, cbdata any) {
		if ref := cur.GetReferrer(); ref != nil && *ref == waitid {
			retev = cur
			cancel()
		}
	}

	err := c.Subscribe(btype, retet, cbf, &retev)
	if err != nil {
		return nil, err
	}

	err = c.Publish(ev)
	if err != nil {
		return nil, err
	}

	select {
	case <-ctx.Done():
		switch ctx.Err() {
		case context.DeadlineExceeded:
			c.logger.Debug("context timeout exceeded")
			return nil, context.DeadlineExceeded
		case context.Canceled:
			c.logger.Debug("context cancelled. whole process is complete")
		}
	}

	defer c.Unsubscribe(btype, retet, cbf, &retev)

	return retev, nil
}
