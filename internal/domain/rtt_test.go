package domain

import (
	"testing"
	"time"
)

// Первое измерение задаёт оценку целиком, дальше она сглаживается.
func TestRTTEstimatorFirstSample(t *testing.T) {
	e := NewRTTEstimator(250*time.Millisecond, time.Millisecond, 2*time.Second)
	if e.RTO() != 250*time.Millisecond {
		t.Fatalf("начальный RTO %s, ожидалось 250ms", e.RTO())
	}

	e.Sample(10 * time.Millisecond)
	// RFC 6298: SRTT = R, RTTVAR = R/2, RTO = SRTT + 4*RTTVAR = 3R
	if got, want := e.RTO(), 30*time.Millisecond; got != want {
		t.Fatalf("RTO после первого измерения %s, ожидалось %s", got, want)
	}
	if e.SRTT() != 10*time.Millisecond {
		t.Fatalf("SRTT %s, ожидалось 10ms", e.SRTT())
	}
}

// Оценка обязана сходиться к реальному обороту, а не оставаться на умолчании:
// именно фиксированный таймаут ронял скорость на канале с потерями.
func TestRTTEstimatorConverges(t *testing.T) {
	e := NewRTTEstimator(250*time.Millisecond, time.Millisecond, 2*time.Second)
	for i := 0; i < 50; i++ {
		e.Sample(4 * time.Millisecond)
	}
	if e.SRTT() != 4*time.Millisecond {
		t.Fatalf("SRTT %s, ожидалось 4ms", e.SRTT())
	}
	if e.RTO() > 10*time.Millisecond {
		t.Fatalf("RTO %s — оценка не сошлась к каналу", e.RTO())
	}
}

// Разброс расширяет таймаут: на дёрганом канале ждать нужно дольше.
func TestRTTEstimatorJitterWidensRTO(t *testing.T) {
	steady := NewRTTEstimator(100*time.Millisecond, time.Millisecond, time.Second)
	jitter := NewRTTEstimator(100*time.Millisecond, time.Millisecond, time.Second)
	for i := 0; i < 30; i++ {
		steady.Sample(10 * time.Millisecond)
		if i%2 == 0 {
			jitter.Sample(2 * time.Millisecond)
		} else {
			jitter.Sample(18 * time.Millisecond)
		}
	}
	if jitter.RTO() <= steady.RTO() {
		t.Fatalf("RTO дёрганого канала %s не больше ровного %s", jitter.RTO(), steady.RTO())
	}
}

func TestRTTEstimatorClampsAndBacksOff(t *testing.T) {
	e := NewRTTEstimator(10*time.Millisecond, 5*time.Millisecond, 40*time.Millisecond)
	e.Sample(time.Microsecond) // оборот меньше нижнего предела
	if e.RTO() != 5*time.Millisecond {
		t.Fatalf("RTO %s, ожидался нижний предел 5ms", e.RTO())
	}
	for i := 0; i < 10; i++ {
		e.BackOff()
	}
	if e.RTO() != 40*time.Millisecond {
		t.Fatalf("RTO %s, ожидался верхний предел 40ms", e.RTO())
	}
	if e.Sample(0); e.RTO() != 40*time.Millisecond {
		t.Fatal("нулевое измерение изменило оценку")
	}
}

// Повторно отправленный пакет не даёт измерения (алгоритм Карна): неизвестно,
// какой из передач отвечает подтверждение.
func TestWindowSamplesOnlyFirstTransmission(t *testing.T) {
	w := NewWindow(BitmapBits)
	wire, _ := (&Packet{Type: TypeData, Seq: 0}).Encode()
	w.Queue(wire)
	w.MarkSent(0, epoch)
	w.MarkSent(0, epoch.Add(time.Millisecond)) // повтор

	w.Ack(1, 0, epoch.Add(5*time.Millisecond), time.Millisecond)
	if rtt, ok := w.TakeRTT(); ok {
		t.Fatalf("снято измерение %s с повторно отправленного пакета", rtt)
	}

	w.Queue(wire)
	w.MarkSent(1, epoch)
	w.Ack(2, 0, epoch.Add(7*time.Millisecond), time.Millisecond)
	rtt, ok := w.TakeRTT()
	if !ok {
		t.Fatal("измерение не снято с пакета, отправленного однажды")
	}
	if rtt != 7*time.Millisecond {
		t.Fatalf("измерение %s, ожидалось 7ms", rtt)
	}
	if _, ok := w.TakeRTT(); ok {
		t.Fatal("одно измерение отдано дважды")
	}
}
