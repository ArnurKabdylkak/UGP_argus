package domain

import (
	"testing"
	"time"
)

var epoch = time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

func ack(mut func(*Packet)) *Packet {
	p := &Packet{Type: TypeAck, Session: 7, AckBase: 5}
	if mut != nil {
		mut(p)
	}
	return p
}

func TestAckGuardAcceptsValid(t *testing.T) {
	g := NewAckGuard(7, 5000, 256)
	if !g.Check(ack(nil), 0, 10, epoch) {
		_, reason := g.Stats()
		t.Fatalf("валидный ACK отклонён: %s", reason)
	}
}

func TestAckGuardRejects(t *testing.T) {
	cases := []struct {
		name string
		pkt  *Packet
	}{
		{"payload в ACK", ack(func(p *Packet) { p.Payload = []byte("leak") })},
		{"seq вне окна", ack(func(p *Packet) { p.AckBase = 999 })},
		{"чужая сессия", ack(func(p *Packet) { p.Session = 8 })},
		{"ненулевые flags", ack(func(p *Packet) { p.Flags = 1 })},
		{"DATA в обратном канале", &Packet{Type: TypeData, Session: 7, Seq: 1, Payload: []byte("x")}},
	}
	for _, c := range cases {
		g := NewAckGuard(7, 5000, 256)
		if g.Check(c.pkt, 0, 10, epoch) {
			t.Errorf("%s: ACK Guard пропустил недопустимый пакет", c.name)
		}
		if n, _ := g.Stats(); n != 1 {
			t.Errorf("%s: счётчик отклонений %d, ожидался 1", c.name, n)
		}
	}
}

// Часы передаются параметром, поэтому лимит частоты проверяется без ожидания.
func TestAckGuardRateLimit(t *testing.T) {
	g := NewAckGuard(7, 1, 3) // 1 ACK/с, burst 3
	passed := 0
	for i := 0; i < 10; i++ {
		if g.Check(ack(nil), 0, 10, epoch) {
			passed++
		}
	}
	if passed != 3 {
		t.Fatalf("пропущено %d ACK, ожидалось 3 (burst)", passed)
	}
	// через секунду возвращается ровно один токен
	if !g.Check(ack(nil), 0, 10, epoch.Add(time.Second)) {
		t.Fatal("токен не восстановился через секунду")
	}
}
