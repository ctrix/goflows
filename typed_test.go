package goflows

import (
	"context"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type orderPlaced struct {
	BaseEvent
	Amount int
}

type orderShipped struct {
	BaseEvent
	Carrier string
}

func newOrderPlaced(amount int) *orderPlaced {
	return &orderPlaced{BaseEvent: NewBaseEvent(scopeEvOrder), Amount: amount}
}

// typedEngine binds scopeEvOrder to *orderPlaced on scopeBusHigh.
func typedEngine(t *testing.T, opts ...EngineOption) *CQRS {
	t.Helper()
	require := require.New(t)
	cq, err := NewCQRSEngine(new(InMemoryTransport), opts...)
	require.NoError(err)
	require.NoError(cq.RegisterBus(scopeBusHigh))
	require.NoError(cq.RegisterBus(scopeBusLow))
	require.NoError(cq.RegisterEvent(scopeBusHigh, scopeEvOrder, OfType[*orderPlaced]()))
	require.NoError(cq.Start())
	return cq
}

// The same event type cannot be bound to two different Go types.
func TestOfTypeRejectsConflictingBinding(t *testing.T) {
	t.Parallel()
	require := require.New(t)
	cq := typedEngine(t)

	// same binding on another bus is fine
	require.NoError(cq.RegisterEvent(scopeBusLow, scopeEvOrder, OfType[*orderPlaced]()))
	// a different Go type for the same event type is not
	require.ErrorIs(cq.RegisterEvent(scopeBusLow, scopeEvOrder+1, OfType[*orderShipped]()), nil)
	require.ErrorIs(cq.RegisterEvent(scopeBusLow, scopeEvOrder, OfType[*orderShipped]()), EEventTypeMismatch)
	require.NoError(cq.Stop())
}

// Publishing a value of the wrong Go type for a bound event type fails.
func TestPublishRejectsWrongGoType(t *testing.T) {
	t.Parallel()
	require := require.New(t)
	cq := typedEngine(t)

	var got int64
	_, err := cq.Subscribe(scopeBusHigh, scopeEvOrder, counterCallback(&got))
	require.NoError(err)

	impostor := &orderShipped{BaseEvent: NewBaseEvent(scopeEvOrder)} // claims scopeEvOrder
	require.ErrorIs(cq.Publish(context.Background(), impostor), EEventTypeMismatch)
	require.NoError(cq.Publish(context.Background(), newOrderPlaced(1)))

	require.NoError(cq.Stop())
	require.Equal(int64(1), got)
}

// Typed Subscribe hands the concrete type to the callback.
func TestTypedSubscribeDeliversConcreteType(t *testing.T) {
	t.Parallel()
	require := require.New(t)
	cq := typedEngine(t)

	var total int64
	sub, err := Subscribe(cq, scopeBusHigh, scopeEvOrder, func(ev *orderPlaced) {
		atomic.AddInt64(&total, int64(ev.Amount))
	})
	require.NoError(err)
	require.Equal(scopeEvOrder, sub.Type())

	require.NoError(cq.Publish(context.Background(), newOrderPlaced(3)))
	require.NoError(cq.Publish(context.Background(), newOrderPlaced(4)))
	require.NoError(cq.Stop())

	require.Equal(int64(7), total)
}

// Typed Subscribe with a type other than the bound one is rejected up front.
func TestTypedSubscribeRejectsWrongGoType(t *testing.T) {
	t.Parallel()
	require := require.New(t)
	cq := typedEngine(t)

	sub, err := Subscribe(cq, scopeBusHigh, scopeEvOrder, func(*orderShipped) {})
	require.ErrorIs(err, EEventTypeMismatch)
	require.Nil(sub)
	require.NoError(cq.Stop())
}

// Without a binding, a typed subscriber skips events of another Go type and
// logs the mismatch; nothing panics.
func TestTypedSubscribeUnboundSkipsOtherTypes(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	rec := newRecordingHandler()
	cq, err := NewCQRSEngine(new(InMemoryTransport), WithLogger(slog.New(rec)))
	require.NoError(err)
	require.NoError(cq.RegisterBus(scopeBusHigh))
	require.NoError(cq.RegisterEvent(scopeBusHigh, scopeEvOrder)) // no OfType
	require.NoError(cq.Start())

	var got int64
	_, err = Subscribe(cq, scopeBusHigh, scopeEvOrder, func(*orderPlaced) { atomic.AddInt64(&got, 1) })
	require.NoError(err)

	require.NoError(cq.Publish(context.Background(), &orderShipped{BaseEvent: NewBaseEvent(scopeEvOrder)}))
	require.NoError(cq.Publish(context.Background(), newOrderPlaced(1)))
	require.NoError(cq.Stop())

	require.Equal(int64(1), got)
	_, logged := rec.find("msg", "event does not match subscription type")
	require.True(logged)
}

// Typed Request returns the reply as its concrete type.
func TestTypedRequest(t *testing.T) {
	t.Parallel()
	require := require.New(t)
	cq := typedEngine(t)
	require.NoError(cq.RegisterEvent(scopeBusHigh, scopeEvReply, OfType[*orderShipped]()))

	_, err := Subscribe(cq, scopeBusHigh, scopeEvOrder, func(req *orderPlaced) {
		reply := &orderShipped{BaseEvent: NewBaseEvent(scopeEvReply), Carrier: "ups"}
		reply.Referrer = req.GetID()
		_ = cq.Publish(context.Background(), reply)
	})
	require.NoError(err)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	shipped, err := Request[*orderShipped](ctx, cq, scopeBusHigh, newOrderPlaced(9), scopeEvReply)
	require.NoError(err)
	require.Equal("ups", shipped.Carrier)
	require.NoError(cq.Stop())
}
