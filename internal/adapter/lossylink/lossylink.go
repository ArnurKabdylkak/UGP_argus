// Package lossylink — обёртка над port.Link, теряющая заданную долю
// отправляемых датаграмм. Нужна для проверки повторной передачи: имитация
// потерь не должна жить в логике протокола.
package lossylink

import (
	"math/rand"
	"sync"
	"time"

	"github.com/argus/udpr/internal/port"
)

// Link оборачивает канал и отбрасывает часть исходящих датаграмм.
type Link struct {
	inner port.Link
	rate  float64

	mu      sync.Mutex
	rng     *rand.Rand
	dropped uint64
}

// Wrap оборачивает канал с долей потерь rate (0..1). При rate <= 0
// возвращается исходный канал без обёртки.
func Wrap(inner port.Link, rate float64) port.Link {
	if rate <= 0 {
		return inner
	}
	return &Link{
		inner: inner,
		rate:  rate,
		rng:   rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (l *Link) Send(payload []byte, to port.Addr) error {
	l.mu.Lock()
	drop := l.rng.Float64() < l.rate
	if drop {
		l.dropped++
	}
	l.mu.Unlock()
	if drop {
		return nil // датаграмма «потеряна в канале»
	}
	return l.inner.Send(payload, to)
}

// Dropped возвращает число отброшенных датаграмм.
func (l *Link) Dropped() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.dropped
}

func (l *Link) Recv(timeout time.Duration) (port.Datagram, error) { return l.inner.Recv(timeout) }
func (l *Link) LocalAddr() port.Addr                              { return l.inner.LocalAddr() }
func (l *Link) Close() error                                      { return l.inner.Close() }
