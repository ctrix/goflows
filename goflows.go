package goflows

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"reflect"
	"runtime/debug"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

const (
	LIBRARY_NAME     = "CQRS"
	EventBusInvalid  = 0
	EventTypeInvalid = 0
)

var (
	EEventTypeExists               = errors.New("event type already exists")
	EEventTypeInvalid              = errors.New("event type is invalid or unknown")
	EEventRegistrationExists       = errors.New("event registration already exists")
	EEventRegistrationDoesntExists = errors.New("event registration doesnt exists")
	EEventTypeDoesntExists         = errors.New("event type does not exist")
	EEventBusExists                = errors.New("event bus already exists")
	EEventBusDoesntExists          = errors.New("event bus does not exists")
	EEventBusInvalid               = errors.New("event bus is invalid")
	EEventBusClosed                = errors.New("event bus is closed")
	EEventNotRegisteredOnBus       = errors.New("event type is not registered on this bus")
	EEventHandlerNull              = errors.New("event handler is null")
	EEventHandlerRedefined         = errors.New("event handler is already set and cannot be redefined")
	EEventHandlerInvalid           = errors.New("event handler is invalid")
	EEventBusNotFound              = errors.New("corresponding event bus not found")
	EOptionInvalid                 = errors.New("option value is invalid")
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

// BusDispatcher consumes one bus and delivers each event to its subscribers.
// It runs `partitions` worker goroutines over the same channel: with one
// partition delivery is sequential and in publish order; with more, up to
// that many events are delivered concurrently and order across events is not
// guaranteed. A single event is always delivered to its subscribers one after
// the other, in subscription order.
type BusDispatcher struct {
	btype EventBus
	cfg   BusConfig
	ch    <-chan EventInterface
	wg    sync.WaitGroup
}

const defaultPartitions = 1

// DefaultBusBufferSize is the queue length of a bus when WithBufferSize is
// not given.
const DefaultBusBufferSize = 111

// BusConfig is the resolved configuration of a bus, built from BusOption
// values by RegisterBus and handed to the transport.
type BusConfig struct {
	// Name is a human readable label used in logs. Optional.
	Name string
	// Partitions is the number of dispatcher goroutines consuming the bus.
	// One preserves publish order; more trades ordering for throughput.
	Partitions int
	// BufferSize is the number of events the bus can hold before Publish
	// blocks. Transports that are not queue based may ignore it.
	BufferSize int
}

// BusOption customises a bus at registration.
type BusOption func(*BusConfig)

// WithBusName labels the bus in logs.
func WithBusName(name string) BusOption {
	return func(c *BusConfig) { c.Name = name }
}

// WithPartitions sets the number of dispatcher goroutines for the bus. Must
// be at least one.
func WithPartitions(n int) BusOption {
	return func(c *BusConfig) { c.Partitions = n }
}

// WithBufferSize sets the bus queue length. Must be at least one.
func WithBufferSize(n int) BusOption {
	return func(c *BusConfig) { c.BufferSize = n }
}

func newBusConfig(opts ...BusOption) BusConfig {
	cfg := BusConfig{
		Partitions: defaultPartitions,
		BufferSize: DefaultBusBufferSize,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	return cfg
}

func (c BusConfig) validate() error {
	if c.Partitions < 1 || c.BufferSize < 1 {
		return EOptionInvalid
	}
	return nil
}

// EventConfig is the resolved configuration of an event type registration.
type EventConfig struct {
	// Name is a human readable label used in logs. Optional.
	Name string
}

// EventOption customises an event type at registration.
type EventOption func(*EventConfig)

// WithEventName labels the event type in logs.
func WithEventName(name string) EventOption {
	return func(c *EventConfig) { c.Name = name }
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

// subscriptionMap is an immutable snapshot of every subscription. Writers
// build a new map and swap the pointer; readers load the pointer and look up
// without any lock, so the hot path never touches shared writable memory.
type subscriptionMap map[subscriptionKey][]*EventSubscriptionObject

type CQRS struct {
	logger *slog.Logger

	handler EventHandlerInterface

	// regMu serialises RegisterEvent writers. Readers use eventTypes directly;
	// the slices stored in it are never mutated once published.
	regMu      sync.Mutex
	eventTypes sync.Map // this is a map[EventType][]EventBus
	eventNames sync.Map // this is a map[EventType]string

	state atomic.Int32 // engineState

	subMu         sync.Mutex // serialises writers only
	subscriptions atomic.Pointer[subscriptionMap]

	subDispatchers sync.Map // this is a map[btype]*BusDispatcher

	// stopCh is closed by Stop once the transport has been stopped. Dispatchers
	// then drain what is still buffered and exit.
	stopCh chan struct{}
}

func NewCQRSEngine(eventh EventHandlerInterface) (*CQRS, error) {
	if eventh == nil {
		return nil, EEventHandlerInvalid
	}

	c := &CQRS{
		handler: eventh,
		stopCh:  make(chan struct{}),
	}
	c.subscriptions.Store(&subscriptionMap{})

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
	if engineState(c.state.Load()) == engineStopped {
		return EEngineStopped
	}

	return nil
}

func (c *CQRS) busDispatcherRun(btype EventBus, cfg BusConfig) error {
	ch, ok := c.handler.Range(btype)
	if !ok {
		return EEventBusDoesntExists
	}

	dis := &BusDispatcher{
		btype: btype,
		cfg:   cfg,
		ch:    ch,
	}

	// Register the dispatcher before starting its goroutines, so that a Stop()
	// racing with RegisterBus() always finds it and waits for it.
	c.subDispatchers.Store(dis.btype, dis)

	dis.wg.Add(cfg.Partitions)
	for i := 0; i < cfg.Partitions; i++ {
		go c.dispatchLoop(dis)
	}

	return nil
}

// dispatchLoop is one partition of a bus dispatcher. It exits when the
// transport closes the channel or, after Stop, once the channel is drained.
func (c *CQRS) dispatchLoop(d *BusDispatcher) {
	defer d.wg.Done()

	for {
		select {
		case ev, ok := <-d.ch:
			if !ok {
				return
			}
			c.deliver(d.btype, ev)
		case <-c.stopCh:
			for {
				select {
				case ev, ok := <-d.ch:
					if !ok {
						return
					}
					c.deliver(d.btype, ev)
				default:
					return
				}
			}
		}
	}
}

// deliver hands ev to every subscriber of (btype, type of ev). A panicking
// subscriber is logged and skipped; it never affects the others or the bus.
func (c *CQRS) deliver(btype EventBus, ev EventInterface) {
	subs := c.subscribersFor(btype, ev.GetType())
	if len(subs) == 0 {
		c.logger.Debug("no subscriptions found for event on bus", "bus-type", btype, "event-type", ev.GetType(), "event-id", ev.GetID())
		return
	}

	for _, sub := range subs {
		c.safeCall(sub, ev)
	}
}

func (c *CQRS) safeCall(sub *EventSubscriptionObject, ev EventInterface) {
	defer func() {
		if r := recover(); r != nil {
			c.logger.Error("subscriber panicked", "bus-type", sub.btype, "event-type", ev.GetType(), "event-name", c.eventName(ev.GetType()), "event-id", ev.GetID(), "panic", r, "stack", string(debug.Stack()))
		}
	}()

	sub.cb(ev, sub.cbdata)
}

func (c *CQRS) RegisterBus(btype EventBus, opts ...BusOption) error {
	if err := c.checkNotStopped(); err != nil {
		return err
	}

	if btype == EventBusInvalid {
		return EEventBusInvalid
	}

	if c.handler == nil {
		return EEventHandlerNull
	}

	cfg := newBusConfig(opts...)
	if err := cfg.validate(); err != nil {
		return err
	}

	if err := c.handler.RegisterBus(btype, cfg); err != nil {
		return err
	}

	c.logger.Debug("registering event bus", "bus-type", btype, "bus-name", cfg.Name, "partitions", cfg.Partitions, "buffer-size", cfg.BufferSize)

	return c.busDispatcherRun(btype, cfg)
}

func (c *CQRS) RegisterEvent(btype EventBus, etype EventType, opts ...EventOption) error {
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

	var cfg EventConfig
	for _, opt := range opts {
		opt(&cfg)
	}

	c.regMu.Lock()
	defer c.regMu.Unlock()

	var sl []EventBus
	if asl, ok := c.eventTypes.Load(etype); ok {
		sl = asl.([]EventBus)
		if inSlice(btype, sl) {
			return EEventRegistrationExists
		}
	}

	// Never append in place: Publish may be iterating the published slice.
	c.eventTypes.Store(etype, append(slices.Clone(sl), btype))
	if cfg.Name != "" {
		c.eventNames.Store(etype, cfg.Name)
	}
	c.logger.Debug("registering event type", "event-type", etype, "event-name", c.eventName(etype), "bus-type", btype)

	return nil
}

func (c *CQRS) Start() error {
	if c.handler == nil {
		return EEventHandlerNull
	}

	if !c.state.CompareAndSwap(int32(engineCreated), int32(engineStarted)) {
		switch engineState(c.state.Load()) {
		case engineStopped:
			return EEngineStopped
		default:
			return EEngineStarted
		}
	}

	c.logger.Debug("starting CQRS engine")

	return nil
}

// Stop shuts the transport down and waits for every bus dispatcher to drain
// the events already published. It can be called in any state and is
// idempotent: the second and later calls return nil without doing anything.
func (c *CQRS) Stop() error {
	if engineState(c.state.Swap(int32(engineStopped))) == engineStopped {
		return nil
	}

	// Stop the transport first: publishers still in flight are released with
	// an error. Then tell the dispatchers to drain and exit.
	c.handler.Stop()
	close(c.stopCh)

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
	var n int64
	for _, subs := range *c.subscriptions.Load() {
		n += int64(len(subs))
	}

	return n
}

// subscribersFor returns the current subscriber list for (btype, etype). Both
// the map and the slice are immutable snapshots, so no lock is needed.
func (c *CQRS) subscribersFor(btype EventBus, etype EventType) []*EventSubscriptionObject {
	return (*c.subscriptions.Load())[subscriptionKey{btype: btype, etype: etype}]
}

// eventName returns the label given with WithEventName, or "" if none.
func (c *CQRS) eventName(etype EventType) string {
	if n, ok := c.eventNames.Load(etype); ok {
		return n.(string)
	}
	return ""
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

	cur := *c.subscriptions.Load()
	m := cur[key]
	for _, s := range m {
		if sameSubscription(s, sub) {
			// Subscription exists, do nothing
			return nil
		}
	}

	// Never mutate the published snapshot: dispatchers may be reading it.
	next := maps.Clone(cur)
	next[key] = append(slices.Clone(m), sub)
	c.subscriptions.Store(&next)

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

	cur := *c.subscriptions.Load()
	m := cur[key]
	for i, s := range m {
		if sameSubscription(s, unsub) {
			next := maps.Clone(cur)
			if newm := slices.Delete(slices.Clone(m), i, i+1); len(newm) == 0 {
				delete(next, key)
			} else {
				next[key] = newm
			}
			c.subscriptions.Store(&next)
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

	buslist, err := c.GetBusTypeFromEvent(ev)
	if err != nil {
		return err
	}

	// Deliver to every bus even if some fail, then report all failures. The
	// caller can still match individual causes with errors.Is.
	var errs []error
	for _, bus := range buslist {
		if err := c.handler.Publish(bus, ev); err != nil {
			errs = append(errs, fmt.Errorf("bus %d: %w", bus, err))
		}
	}

	return errors.Join(errs...)
}

// PublishAndWait publishes ev and waits for a reply of type retet on bus btype
// whose Referrer is the ID of ev. The first matching reply is returned and any
// later one is dropped. On timeout the error is context.DeadlineExceeded. The
// temporary reply subscription is always removed before returning.
func (c *CQRS) PublishAndWait(btype EventBus, ev EventInterface, retet EventType, timeout time.Duration) (EventInterface, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// Buffered by one so the dispatcher never blocks on us; the non-blocking
	// send makes the first reply win and discards the rest without touching
	// any shared variable.
	reply := make(chan EventInterface, 1)
	waitid := ev.GetID()
	cbf := func(cur EventInterface, _ any) {
		if ref := cur.GetReferrer(); ref != nil && *ref == waitid {
			select {
			case reply <- cur:
			default:
			}
		}
	}

	// Subscribe before publishing so a fast reply cannot be missed.
	if err := c.Subscribe(btype, retet, cbf, nil); err != nil {
		return nil, err
	}
	defer c.Unsubscribe(btype, retet, cbf, nil)

	if err := c.Publish(ev); err != nil {
		return nil, err
	}

	select {
	case res := <-reply:
		return res, nil
	case <-ctx.Done():
		c.logger.Debug("request timed out", "event-id", waitid, "reply-type", retet, "bus-type", btype)
		return nil, ctx.Err()
	}
}
