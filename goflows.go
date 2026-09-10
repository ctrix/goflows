package goflows

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
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

type CQRS struct {
	logger *slog.Logger

	handler    EventHandlerInterface
	eventTypes sync.Map // this is a map[EventType][]EventBus
	busData    sync.Map // this is a map[EventBus]*BusDispatcher

	subcount       int64
	subChan        chan *EventSubscriptionObject
	unsubChan      chan *EventSubscriptionObject
	subCtx         context.Context
	subCancel      context.CancelFunc
	subscriptions  sync.Map // this is a map[subscriptionKey][]*EventSubscriptionObject
	subDispatchers sync.Map // this is a map[btype]*BusDispatcher
}

func NewCQRSEngine(eventh EventHandlerInterface) (*CQRS, error) {
	if eventh == nil {
		return nil, EEventHandlerInvalid
	}

	c := &CQRS{
		handler:   eventh,
		subcount:  0,
		subChan:   make(chan *EventSubscriptionObject),
		unsubChan: make(chan *EventSubscriptionObject),
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
					many, ok := c.subscriptions.Load(subscriptionKey{btype: d.btype, etype: ev.GetType()})
					if !ok {
						// There are no subscriptions.
						if !warned {
							// warned = true
							c.logger.Warn("no subscriptions found for event on bus", "bus-type", d.btype, "event-type", ev.GetType())
						}
					} else {
						subs := many.([]*EventSubscriptionObject)
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
	if etype == EventBusInvalid {
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

	// Subscription/Unsubscription handler
	c.subCtx, c.subCancel = context.WithCancel(context.Background())
	go c.subscriptionsHandler()

	c.logger.Debug("starting CQRS engine")

	return nil
}

func (c *CQRS) Stop() error {
	c.handler.Stop()

	c.subDispatchers.Range(func(k, v interface{}) bool {
		d := v.(*BusDispatcher)
		d.wg.Wait()
		return true
	})

	c.subCancel()

	return nil
}

// This function is used only in tests
func (c *CQRS) countSubscriptions() int64 {
	return atomic.LoadInt64(&c.subcount)
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

	if err := c.checkEventOnBus(btype, etype); err != nil {
		return err
	}

	sub := &EventSubscriptionObject{
		btype:  btype,
		etype:  etype,
		cb:     cb,
		cbdata: cbdata,
	}

	sub.ctx, sub.cancel = context.WithCancel(context.Background())

	m := sync.Mutex{}
	sub.cond = sync.NewCond(&m)

	sub.cond.L.Lock()
	c.subChan <- sub

	sub.cond.Wait()
	sub.cond.L.Unlock()

	return nil
}

func (c *CQRS) Unsubscribe(btype EventBus, etype EventType, cb EventSubscriptionCallback, cbdata any) error {
	if etype == EventTypeInvalid {
		return EEventTypeInvalid
	}

	if cb == nil {
		return EUnsubscriptionInvalid
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

	m := sync.Mutex{}
	unsub.cond = sync.NewCond(&m)

	unsub.cond.L.Lock()
	c.unsubChan <- unsub

	unsub.cond.Wait()
	unsub.cond.L.Unlock()

	return nil
}

func (c *CQRS) subscriptionsHandler() {
	for {
		select {
		case <-c.subCtx.Done():
			// Do not leak subroutines
			return
		case sub := <-c.subChan:
			func() {
				defer func() {
					sub.cond.L.Lock()
					sub.cond.Signal()
					sub.cond.L.Unlock()
				}()

				c.logger.Info("Subscribing", "bus-type", sub.btype, "sub-type", sub.etype)

				key := subscriptionKey{btype: sub.btype, etype: sub.etype}
				first := []*EventSubscriptionObject{sub}
				many, loaded := c.subscriptions.LoadOrStore(key, first)
				if !loaded {
					atomic.AddInt64(&c.subcount, 1)
				} else {
					found := false

					m := many.([]*EventSubscriptionObject)
					for _, s := range m {
						if reflect.ValueOf(s.cb) == reflect.ValueOf(sub.cb) && s.cbdata == sub.cbdata {
							// Subscription exists, do nothing
							found = true
							break
						}
					}

					// Subscription does not exists, add it
					if !found {
						newm := append(m, sub)
						c.subscriptions.Store(key, newm)
						atomic.AddInt64(&c.subcount, 1)
					}
				}
			}()
		case unsub := <-c.unsubChan:
			func() {
				defer func() {
					unsub.cond.L.Lock()
					unsub.cond.Signal()
					unsub.cond.L.Unlock()
				}()
				c.logger.Info("Unsubscribing", "bus-type", unsub.btype, "unsub-type", unsub.etype)

				key := subscriptionKey{btype: unsub.btype, etype: unsub.etype}
				many, loaded := c.subscriptions.Load(key)
				if !loaded {
					// Subscription does not exists, cannot unsubscribe
				} else {
					m := many.([]*EventSubscriptionObject)

					for i, s := range m {
						if reflect.ValueOf(s.cb) == reflect.ValueOf(unsub.cb) && s.cbdata == unsub.cbdata {
							// Subscription exists
							s.cancel()
							newm := append(m[:i], m[i+1:]...)
							c.subscriptions.Store(key, newm)
							atomic.AddInt64(&c.subcount, -1)
							break
						}
					}
					// Subscription does not exists, do nothing
				}
			}()
		}
	}
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
