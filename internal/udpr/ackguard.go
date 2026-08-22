package udpr

import (
	"fmt"
	"sync"
	"time"
)

// AckGuard делает обратный канал управляемым, а не свободным:
// фиксированная длина ACK, только ожидаемые sequence numbers, bitmap
// ограниченного размера, запрет произвольного payload, лимит частоты.
// Отбрасывание неизвестных типов выполняет Decode.
type AckGuard struct {
	Session   uint32
	RateLimit float64 // ACK в секунду
	Burst     float64

	mu         sync.Mutex
	tokens     float64
	last       time.Time
	rejected   uint64
	lastReason string
}

// NewAckGuard создаёт guard для сессии с ограничением частоты ACK.
func NewAckGuard(session uint32, rateLimit, burst float64) *AckGuard {
	return &AckGuard{
		Session:   session,
		RateLimit: rateLimit,
		Burst:     burst,
		tokens:    burst,
		last:      time.Now(),
	}
}

// Check проверяет ACK против политики. Допустимый диапазон ack_base —
// [expectLo, expectHi] включительно.
func (g *AckGuard) Check(p *Packet, expectLo, expectHi uint32) bool {
	switch {
	case p.Type != TypeAck:
		return g.reject("не ACK в обратном канале: " + TypeName(p.Type))
	case p.Session != g.Session:
		return g.reject("чужой session_id")
	case len(p.Payload) != 0:
		return g.reject("произвольный payload в ACK")
	case p.Flags != 0:
		return g.reject("ненулевые flags")
	case p.AckBase < expectLo || p.AckBase > expectHi:
		return g.reject(fmt.Sprintf("ack_base=%d вне окна [%d,%d]", p.AckBase, expectLo, expectHi))
	}

	g.mu.Lock()
	defer g.mu.Unlock()
	now := time.Now()
	g.tokens += now.Sub(g.last).Seconds() * g.RateLimit
	if g.tokens > g.Burst {
		g.tokens = g.Burst
	}
	g.last = now
	if g.tokens < 1 {
		g.rejected++
		g.lastReason = "превышен лимит частоты ACK"
		return false
	}
	g.tokens--
	return true
}

// Drop учитывает пакет, отброшенный до проверок (битый CRC, неизвестный тип).
func (g *AckGuard) Drop(reason string) {
	g.reject(reason)
}

func (g *AckGuard) reject(reason string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.rejected++
	g.lastReason = reason
	return false
}

// Stats возвращает число отклонённых пакетов и последнюю причину.
func (g *AckGuard) Stats() (uint64, string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.rejected, g.lastReason
}
