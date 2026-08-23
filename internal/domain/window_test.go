package domain

import (
	"testing"
	"time"
)

func TestWindowQueueAndAck(t *testing.T) {
	w := NewWindow(8)
	now := epoch
	for i := 0; i < 4; i++ {
		wire, _ := (&Packet{Type: TypeData, Seq: uint32(i)}).Encode()
		if got := w.Queue(wire); got != uint32(i) {
			t.Fatalf("Queue вернул seq=%d, ожидался %d", got, i)
		}
		w.MarkSent(uint32(i), now)
	}
	if w.InFlight() != 4 || w.Empty() {
		t.Fatalf("в полёте %d пакетов, ожидалось 4", w.InFlight())
	}

	// подтверждены 0 и 1, seq=2 потерян, seq=3 получен
	w.Ack(2, 1<<0, now, time.Second)
	if w.Base() != 2 {
		t.Fatalf("base=%d, ожидалось 2", w.Base())
	}
	if _, ok := w.Wire(0); ok {
		t.Fatal("подтверждённый seq=0 остался в окне")
	}
	if _, ok := w.Wire(2); !ok {
		t.Fatal("неподтверждённый seq=2 пропал из окна")
	}
	if w.InFlight() != 1 {
		t.Fatalf("в полёте %d пакетов, ожидался 1 (seq=2)", w.InFlight())
	}
}

// Кольцо адресуется по seq % BitmapBits, а подтверждение вправе назвать номер
// на BitmapBits вперёд. Такой seq попадает в тот же слот и не должен
// притвориться живым пакетом.
func TestWindowRejectsSlotCollision(t *testing.T) {
	w := NewWindow(BitmapBits)
	wire, _ := (&Packet{Type: TypeData, Seq: 0}).Encode()
	w.Queue(wire)

	collides := uint32(BitmapBits) // тот же слот, что и seq=0
	if _, ok := w.Wire(collides); ok {
		t.Fatalf("seq=%d выдал себя за живой seq=0", collides)
	}
	if n := w.Tries(collides); n != 0 {
		t.Fatalf("Tries(%d)=%d, ожидался 0", collides, n)
	}
	if n := w.MarkSent(collides, epoch); n != 0 {
		t.Fatalf("MarkSent(%d)=%d — отмечена чужая попытка", collides, n)
	}
	if _, ok := w.Wire(0); !ok {
		t.Fatal("живой seq=0 потерян")
	}
}

func TestWindowExpiredSorted(t *testing.T) {
	w := NewWindow(BitmapBits)
	for i := 0; i < 5; i++ {
		wire, _ := (&Packet{Type: TypeData, Seq: uint32(i)}).Encode()
		w.Queue(wire)
		w.MarkSent(uint32(i), epoch)
	}
	got := w.Expired(epoch.Add(time.Second), 100*time.Millisecond)
	if len(got) != 5 {
		t.Fatalf("истекло %d пакетов, ожидалось 5", len(got))
	}
	for i, seq := range got {
		if seq != uint32(i) {
			t.Fatalf("порядок нарушен: %v", got)
		}
	}
	if none := w.Expired(epoch, time.Second); len(none) != 0 {
		t.Fatalf("до таймаута вернулось %v", none)
	}
}

// CanQueue считает занятость от base: выборочно подтверждённые пакеты не
// должны позволять уйти за окно приёма.
func TestWindowCanQueueFromBase(t *testing.T) {
	w := NewWindow(4)
	for i := 0; i < 4; i++ {
		wire, _ := (&Packet{Type: TypeData, Seq: uint32(i)}).Encode()
		w.Queue(wire)
	}
	if w.CanQueue() {
		t.Fatal("окно заполнено, а CanQueue разрешает отправку")
	}
	w.Ack(0, 1<<0, epoch, time.Second) // подтверждён seq=1, base не двинулся
	if w.CanQueue() {
		t.Fatal("подтверждение дырки сдвинуло окно за base")
	}
	w.Ack(2, 0, epoch, time.Second)
	if !w.CanQueue() {
		t.Fatal("base продвинулся, а место в окне не освободилось")
	}
}
