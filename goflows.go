package goflows

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"reflect"
	"runtime/debug"
	"slices"
	"sync"
	"sync/atomic"
)

const (
	libraryName      = "goflows"
	EventBusInvalid  = 0
	EventTypeInvalid = 0
)

// Sentinel errors. Compare with errors.Is: Publish may wrap them with the bus
// they came from and join several.
var (
	ErrTransportNil        = errors.New("transport is nil")
	ErrBusInvalid          = errors.New("bus is invalid")
	ErrBusExists           = errors.New("bus already exists")
	ErrBusNotFound         = errors.New("bus does not exist")
	ErrBusClosed           = errors.New("bus is closed")
	ErrEventTypeInvalid    = errors.New("event type is invalid or unknown")
	ErrEventTypeNotFound   = errors.New("event type is not registered")
	ErrEventRegistered     = errors.New("event type is already registered on this bus")
	ErrEventNotOnBus       = errors.New("event type is not registered on this bus")
	ErrEventTypeMismatch   = errors.New("event type is bound to a different Go type")
	ErrOptionInvalid       = errors.New("option value is invalid")
	ErrEngineStarted       = errors.New("engine already started")
	ErrEngineStopped       = errors.New("engine is stopped")
	ErrSubscriptionInvalid = errors.New("subscription request is invalid")
)

// subscriptionKey identifies the set of subscribers listening for a given
// event type on a given bus. Subscriptions are scoped to the bus: a subscriber
// on bus A never sees copies of the same event delivered on bus B.
type subscriptionKey struct {
	btype EventBus
	etype EventType
}

// busDispatcher consumes one bus and delivers each event to its subscribers.
// It runs `partitions` worker goroutines over the same channel: with one
// partition delivery is sequential and in publish order; with more, up to
// that many events are delivered concurrently and order across events is not
// guaranteed. A single event is always delivered to its subscribers one after
// the other, in subscription order.
type busDispatcher struct {
	btype EventBus
	cfg   BusConfig
	ch    <-chan Event
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
		return ErrOptionInvalid
	}
	return nil
}

// EventConfig is the resolved configuration of an event type registration.
type EventConfig struct {
	// Name is a human readable label used in logs. Optional.
	Name string

	goType reflect.Type
}

// OfType binds the event type being registered to the Go type T. Once bound,
// registering the same event type with another Go type, publishing a value of
// another Go type with that event type, or subscribing with [Subscribe] for
// another T fails with ErrEventTypeMismatch. Use it so two packages that happen
// to pick the same EventType value fail loudly instead of receiving each
// other's events.
func OfType[T Event]() EventOption {
	return func(c *EventConfig) { c.goType = reflect.TypeFor[T]() }
}

// EventOption customises an event type at registration.
type EventOption func(*EventConfig)

// WithEventName labels the event type in logs.
func WithEventName(name string) EventOption {
	return func(c *EventConfig) { c.Name = name }
}

// engineState is the lifecycle of an Engine: created -> started -> stopped.
// Registration and subscription work in both created and started; nothing
// works once stopped.
type engineState int32

const (
	engineCreated engineState = iota
	engineStarted
	engineStopped
)

// eventRegistration is what RegisterEvent records for one event type.
type eventRegistration struct {
	buses  []EventBus   // never mutated once published
	name   string       // from WithEventName, "" if none
	goType reflect.Type // from OfType, nil if unbound
}

// eventRegistry is an immutable snapshot of every event registration, keyed
// by event type. Writers build a new map and swap the pointer; readers load
// the pointer and look up without any lock.
type eventRegistry map[EventType]eventRegistration

// subscriptionMap is an immutable snapshot of every subscription. Writers
// build a new map and swap the pointer; readers load the pointer and look up
// without any lock, so the hot path never touches shared writable memory.
type subscriptionMap map[subscriptionKey][]*Subscription

// Subscription is the handle returned by Subscribe. Each call to Subscribe
// creates a distinct subscription, even for the same callback; identity is the
// handle itself, which is what Unsubscribe removes.
type Subscription struct {
	engine *Engine
	key    subscriptionKey
	cb     EventSubscriptionCallback
}

// Bus returns the bus the subscription listens on.
func (s *Subscription) Bus() EventBus { return s.key.btype }

// Type returns the event type the subscription listens for.
func (s *Subscription) Type() EventType { return s.key.etype }

// Unsubscribe removes the subscription. It is idempotent: removing an already
// removed subscription returns nil. After Stop it returns ErrEngineStopped.
func (s *Subscription) Unsubscribe() error {
	return s.engine.unsubscribe(s)
}

// BindContext removes the subscription when ctx is done, from a separate
// goroutine as [context.AfterFunc] does. It returns s for chaining. A
// subscription removed by hand first is left alone; an error from the late
// Unsubscribe, such as ErrEngineStopped, is ignored.
func (s *Subscription) BindContext(ctx context.Context) *Subscription {
	context.AfterFunc(ctx, func() { _ = s.Unsubscribe() })
	return s
}

type Engine struct {
	logger *slog.Logger

	transport Transport

	// regMu serialises RegisterEvent writers. Readers load the registry
	// snapshot without a lock; snapshots are never mutated once published.
	regMu  sync.Mutex
	events atomic.Pointer[eventRegistry]

	state atomic.Int32 // engineState

	subMu         sync.Mutex // serialises writers only
	subscriptions atomic.Pointer[subscriptionMap]

	subDispatchers sync.Map // this is a map[btype]*busDispatcher

	// stopCh is closed by Stop once the transport has been stopped. Dispatchers
	// then drain what is still buffered and exit.
	stopCh chan struct{}
}

// EngineOption customises the engine at construction.
type EngineOption func(*engineConfig)

type engineConfig struct {
	logger *slog.Logger
}

// WithLogger sets the logger used by the engine and handed to the transport.
// By default the library logs nothing. A nil logger is ignored.
func WithLogger(l *slog.Logger) EngineOption {
	return func(c *engineConfig) {
		if l != nil {
			c.logger = l
		}
	}
}

// NewEngine builds an engine on top of the given transport. If the
// transport implements [LoggerSetter] it receives the engine logger.
func NewEngine(eventh Transport, opts ...EngineOption) (*Engine, error) {
	if eventh == nil {
		return nil, ErrTransportNil
	}

	cfg := engineConfig{logger: slog.New(slog.DiscardHandler)}
	for _, opt := range opts {
		opt(&cfg)
	}

	c := &Engine{
		logger:    cfg.logger.With("library", libraryName),
		transport: eventh,
		stopCh:    make(chan struct{}),
	}
	c.subscriptions.Store(&subscriptionMap{})
	c.events.Store(&eventRegistry{})

	if ls, ok := eventh.(LoggerSetter); ok {
		ls.SetLogger(cfg.logger)
	}

	return c, nil
}

// checkNotStopped returns ErrEngineStopped once Stop has been called.
func (c *Engine) checkNotStopped() error {
	if engineState(c.state.Load()) == engineStopped {
		return ErrEngineStopped
	}

	return nil
}

func (c *Engine) busDispatcherRun(btype EventBus, cfg BusConfig) error {
	ch, ok := c.transport.Stream(btype)
	if !ok {
		return ErrBusNotFound
	}

	dis := &busDispatcher{
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
func (c *Engine) dispatchLoop(d *busDispatcher) {
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
func (c *Engine) deliver(btype EventBus, ev Event) {
	subs := c.subscribersFor(btype, ev.GetType())
	if len(subs) == 0 {
		c.logger.Debug("no subscriptions found for event on bus", "bus-type", btype, "event-type", ev.GetType(), "event-id", ev.GetID())
		return
	}

	for _, sub := range subs {
		c.safeCall(sub, ev)
	}
}

func (c *Engine) safeCall(sub *Subscription, ev Event) {
	defer func() {
		if r := recover(); r != nil {
			c.logger.Error("subscriber panicked", "bus-type", sub.key.btype, "event-type", ev.GetType(), "event-name", c.eventName(ev.GetType()), "event-id", ev.GetID(), "panic", r, "stack", string(debug.Stack()))
		}
	}()

	sub.cb(ev)
}

func (c *Engine) RegisterBus(btype EventBus, opts ...BusOption) error {
	if err := c.checkNotStopped(); err != nil {
		return err
	}

	if btype == EventBusInvalid {
		return ErrBusInvalid
	}

	if c.transport == nil {
		return ErrTransportNil
	}

	cfg := newBusConfig(opts...)
	if err := cfg.validate(); err != nil {
		return err
	}

	if err := c.transport.Open(btype, cfg); err != nil {
		return err
	}

	c.logger.Debug("registering event bus", "bus-type", btype, "bus-name", cfg.Name, "partitions", cfg.Partitions, "buffer-size", cfg.BufferSize)

	return c.busDispatcherRun(btype, cfg)
}

func (c *Engine) RegisterEvent(btype EventBus, etype EventType, opts ...EventOption) error {
	if err := c.checkNotStopped(); err != nil {
		return err
	}

	if etype == EventTypeInvalid {
		return ErrEventTypeInvalid
	}

	if !c.transport.Has(btype) {
		return ErrBusNotFound
	}

	var cfg EventConfig
	for _, opt := range opts {
		opt(&cfg)
	}

	c.regMu.Lock()
	defer c.regMu.Unlock()

	cur := *c.events.Load()
	reg := cur[etype]

	if cfg.goType != nil && reg.goType != nil && reg.goType != cfg.goType {
		return ErrEventTypeMismatch
	}

	if slices.Contains(reg.buses, btype) {
		return ErrEventRegistered
	}

	// Never mutate the published snapshot: Publish may be reading it.
	reg.buses = append(slices.Clone(reg.buses), btype)
	if cfg.Name != "" {
		reg.name = cfg.Name
	}
	if cfg.goType != nil {
		reg.goType = cfg.goType
	}

	next := maps.Clone(cur)
	next[etype] = reg
	c.events.Store(&next)

	c.logger.Debug("registering event type", "event-type", etype, "event-name", reg.name, "bus-type", btype)

	return nil
}

func (c *Engine) Start() error {
	if c.transport == nil {
		return ErrTransportNil
	}

	if !c.state.CompareAndSwap(int32(engineCreated), int32(engineStarted)) {
		switch engineState(c.state.Load()) {
		case engineStopped:
			return ErrEngineStopped
		default:
			return ErrEngineStarted
		}
	}

	c.logger.Debug("starting engine")

	return nil
}

// Stop closes the transport and waits for every bus dispatcher to drain the
// events already published. It can be called in any state and is idempotent:
// the second and later calls return nil without doing anything. An error from
// the transport Close is returned after the drain.
func (c *Engine) Stop() error {
	if engineState(c.state.Swap(int32(engineStopped))) == engineStopped {
		return nil
	}

	// Close the transport first: publishers still in flight are released with
	// an error. Then tell the dispatchers to drain and exit.
	closeErr := c.transport.Close()
	close(c.stopCh)

	c.subDispatchers.Range(func(k, v interface{}) bool {
		d := v.(*busDispatcher)
		d.wg.Wait()
		return true
	})

	c.logger.Debug("engine stopped")

	return closeErr
}

// subscribersFor returns the current subscriber list for (btype, etype). Both
// the map and the slice are immutable snapshots, so no lock is needed.
func (c *Engine) subscribersFor(btype EventBus, etype EventType) []*Subscription {
	return (*c.subscriptions.Load())[subscriptionKey{btype: btype, etype: etype}]
}

// registration returns the current registration of etype, if any. The value
// is an immutable snapshot: safe to read without a lock.
func (c *Engine) registration(etype EventType) (eventRegistration, bool) {
	reg, ok := (*c.events.Load())[etype]
	return reg, ok
}

// checkGoType returns ErrEventTypeMismatch when etype is bound with OfType to a
// Go type other than got. An unbound etype accepts anything.
func (c *Engine) checkGoType(etype EventType, got reflect.Type) error {
	if reg, ok := c.registration(etype); ok && reg.goType != nil && reg.goType != got {
		return ErrEventTypeMismatch
	}
	return nil
}

// eventName returns the label given with WithEventName, or "" if none.
func (c *Engine) eventName(etype EventType) string {
	reg, _ := c.registration(etype)
	return reg.name
}

// checkEventOnBus verifies that btype is a registered bus and that etype has
// been registered on that specific bus.
func (c *Engine) checkEventOnBus(btype EventBus, etype EventType) error {
	if btype == EventBusInvalid || !c.transport.Has(btype) {
		return ErrBusNotFound
	}

	reg, ok := c.registration(etype)
	if !ok {
		return ErrEventTypeNotFound
	}

	if !slices.Contains(reg.buses, btype) {
		return ErrEventNotOnBus
	}

	return nil
}

// subscribe registers cb for events of type etype travelling on bus btype and
// returns a handle to remove it. The bus must exist and etype must be
// registered on it. Every call creates a new subscription.
func (c *Engine) subscribe(btype EventBus, etype EventType, cb EventSubscriptionCallback) (*Subscription, error) {
	if etype == EventTypeInvalid {
		return nil, ErrEventTypeInvalid
	}

	if cb == nil {
		return nil, ErrSubscriptionInvalid
	}

	if err := c.checkNotStopped(); err != nil {
		return nil, err
	}

	if err := c.checkEventOnBus(btype, etype); err != nil {
		return nil, err
	}

	sub := &Subscription{
		engine: c,
		key:    subscriptionKey{btype: btype, etype: etype},
		cb:     cb,
	}

	c.subMu.Lock()
	defer c.subMu.Unlock()

	c.logger.Debug("subscribing", "bus-type", btype, "sub-type", etype)

	// Never mutate the published snapshot: dispatchers may be reading it.
	cur := *c.subscriptions.Load()
	next := maps.Clone(cur)
	next[sub.key] = append(slices.Clone(cur[sub.key]), sub)
	c.subscriptions.Store(&next)

	return sub, nil
}

// Subscribe registers fn for events of type etype travelling on bus btype and
// returns a handle to remove it. fn receives the event as T: use the concrete
// type to skip the type assertion, or [Event] to receive everything.
//
// The bus must exist and etype must be registered on it. If etype is bound
// with [OfType] to a concrete type other than T the call fails with
// ErrEventTypeMismatch; an interface T is never checked. If etype is not
// bound, an event that is not a T is logged and skipped. Every call creates a
// new subscription.
func Subscribe[T Event](c *Engine, btype EventBus, etype EventType, fn func(T)) (*Subscription, error) {
	if fn == nil {
		return nil, ErrSubscriptionInvalid
	}

	want := reflect.TypeFor[T]()
	if want.Kind() != reflect.Interface {
		if err := c.checkGoType(etype, want); err != nil {
			return nil, err
		}
	}

	return c.subscribe(btype, etype, func(ev Event) {
		t, ok := ev.(T)
		if !ok {
			c.logger.Error("event does not match subscription type", "bus-type", btype, "event-type", etype, "event-id", ev.GetID(), "want", want.String(), "got", reflect.TypeOf(ev).String())
			return
		}
		fn(t)
	})
}

// Request publishes ev and waits for a reply of type retet on bus btype whose
// Referrer is the ID of ev, returned as T. Use the concrete reply type, or
// [Event] for any. The first matching reply wins and later ones are dropped.
// When ctx is done first the error is ctx.Err(): context.DeadlineExceeded for
// a timeout, context.Canceled for an external cancellation. A reply that is
// not a T fails with ErrEventTypeMismatch. The temporary reply subscription is
// always removed before returning.
func Request[T Event](ctx context.Context, c *Engine, btype EventBus, ev Event, retet EventType) (T, error) {
	var zero T

	res, err := c.request(ctx, btype, ev, retet)
	if err != nil {
		return zero, err
	}

	t, ok := res.(T)
	if !ok {
		return zero, ErrEventTypeMismatch
	}

	return t, nil
}

func (c *Engine) unsubscribe(sub *Subscription) error {
	if err := c.checkNotStopped(); err != nil {
		return err
	}

	c.subMu.Lock()
	defer c.subMu.Unlock()

	cur := *c.subscriptions.Load()
	m := cur[sub.key]
	i := slices.Index(m, sub)
	if i < 0 {
		// Already removed, nothing to do
		return nil
	}

	c.logger.Debug("unsubscribing", "bus-type", sub.key.btype, "unsub-type", sub.key.etype)

	next := maps.Clone(cur)
	if newm := slices.Delete(slices.Clone(m), i, i+1); len(newm) == 0 {
		delete(next, sub.key)
	} else {
		next[sub.key] = newm
	}
	c.subscriptions.Store(&next)

	return nil
}

// GetBusTypeFromEventType returns the buses etype is registered on. The slice
// is an immutable snapshot and must not be modified.
func (c *Engine) GetBusTypeFromEventType(etype EventType) ([]EventBus, error) {
	reg, ok := c.registration(etype)
	if !ok {
		return nil, ErrEventTypeInvalid
	}

	return reg.buses, nil
}

func (c *Engine) GetBusTypeFromEvent(ev Event) ([]EventBus, error) {
	etype := ev.GetType()
	return c.GetBusTypeFromEventType(etype)
}

// Publish delivers ev to every bus its type is registered on. It blocks while
// a bus is full, until ctx is done: the transport returns ctx.Err() in that
// case. Use a context with a deadline when publishing from inside a subscriber
// of the same bus, otherwise a full bus deadlocks the dispatcher.
func (c *Engine) Publish(ctx context.Context, ev Event) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if err := c.checkNotStopped(); err != nil {
		return err
	}

	// One registry load serves both the Go type check and the bus list.
	reg, ok := c.registration(ev.GetType())
	if !ok {
		return ErrEventTypeInvalid
	}

	if reg.goType != nil && reg.goType != reflect.TypeOf(ev) {
		return ErrEventTypeMismatch
	}

	buslist := reg.buses

	// Deliver to every bus even if some fail, then report all failures. The
	// caller can still match individual causes with errors.Is.
	var errs []error
	for _, bus := range buslist {
		if err := c.transport.Publish(ctx, bus, ev); err != nil {
			errs = append(errs, fmt.Errorf("bus %d: %w", bus, err))
		}
	}

	return errors.Join(errs...)
}

// request is the untyped core of [Request].
func (c *Engine) request(ctx context.Context, btype EventBus, ev Event, retet EventType) (Event, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// Buffered by one so the dispatcher never blocks on us; the non-blocking
	// send makes the first reply win and discards the rest without touching
	// any shared variable.
	reply := make(chan Event, 1)
	waitid := ev.GetID()
	cbf := func(cur Event) {
		if cur.GetReferrer() == waitid {
			select {
			case reply <- cur:
			default:
			}
		}
	}

	// Subscribe before publishing so a fast reply cannot be missed.
	sub, err := c.subscribe(btype, retet, cbf)
	if err != nil {
		return nil, err
	}
	defer sub.Unsubscribe()

	if err := c.Publish(ctx, ev); err != nil {
		return nil, err
	}

	select {
	case res := <-reply:
		return res, nil
	case <-ctx.Done():
		c.logger.Debug("request abandoned", "event-id", waitid, "reply-type", retet, "bus-type", btype, "reason", ctx.Err())
		return nil, ctx.Err()
	}
}
