package usecase

import (
	"bytes"
	"testing"
	"time"

	"github.com/argus/udpr/internal/adapter/sysclock"
)

// Сторож тишины не должен падать на коротком -idle: NewTicker паникует
// на неположительном интервале, а idle приходит из флага командной строки.
func TestWatchdogIntervalPositive(t *testing.T) {
	for _, idle := range []time.Duration{1, 3, time.Microsecond, time.Millisecond, 10 * time.Second} {
		if got := watchdogInterval(idle); got <= 0 {
			t.Errorf("idle=%v: интервал сторожа %v, ожидался положительный", idle, got)
		}
	}
}

// Тот же случай целиком: приём с наносекундным idle завершается по тишине,
// а не паникой.
func TestReceiverTinyIdleTimeout(t *testing.T) {
	r := NewReceiver(listen(t, 0), sysclock.New(), ReceiverConfig{IdleTimeout: 3})

	done := make(chan struct{})
	go func() {
		defer close(done)
		var sink bytes.Buffer
		r.ReceiveStream(t.Context(), &sink)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("приёмник не вышел по тишине")
	}
}
