package goflows

import (
	"context"
	"testing"
)

func benchEngine(b *testing.B) *CQRS {
	b.Helper()
	eh := new(InMemoryEventHandler)
	cq, err := NewCQRSEngine(eh)
	if err != nil {
		b.Fatal(err)
	}
	if err := cq.RegisterBus(scopeBusHigh); err != nil {
		b.Fatal(err)
	}
	if err := cq.RegisterEvent(scopeBusHigh, scopeEvOrder); err != nil {
		b.Fatal(err)
	}
	if err := cq.Start(); err != nil {
		b.Fatal(err)
	}
	if _, err := cq.Subscribe(scopeBusHigh, scopeEvOrder, func(Event) {}); err != nil {
		b.Fatal(err)
	}
	return cq
}

// Cost of the lifecycle check alone, as paid by every Publish.
func BenchmarkCheckNotStopped(b *testing.B) {
	cq := benchEngine(b)
	defer cq.Stop()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = cq.checkNotStopped()
		}
	})
}

// Cost of the subscriber lookup alone, as paid by every dispatched event.
func BenchmarkSubscribersFor(b *testing.B) {
	cq := benchEngine(b)
	defer cq.Stop()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = cq.subscribersFor(scopeBusHigh, scopeEvOrder)
		}
	})
}

// End to end: Publish through the in-memory transport to one subscriber.
func BenchmarkPublish(b *testing.B) {
	cq := benchEngine(b)
	ev := newScopeEvent(scopeEvOrder)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = cq.Publish(context.Background(), ev)
		}
	})
	b.StopTimer()
	cq.Stop()
}
