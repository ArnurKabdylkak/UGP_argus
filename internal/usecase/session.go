package usecase

import (
	"fmt"
	"io"
	"time"

	"github.com/argus/udpr/internal/domain"
	"github.com/argus/udpr/internal/port"
)

// ReceiverStats — счётчики по итогам приёма одного потока.
type ReceiverStats struct {
	Received   uint64
	Dup        uint64
	OutOfOrder uint64
	Rejected   uint64
	Acks       uint64
	Bytes      uint64
}

func (s ReceiverStats) String() string {
	return fmt.Sprintf("received=%d dup=%d out_of_order=%d rejected=%d acks=%d bytes=%d",
		s.Received, s.Dup, s.OutOfOrder, s.Rejected, s.Acks, s.Bytes)
}

// sessionKey различает потоки: один и тот же session_id от разных источников —
// это разные сессии.
type sessionKey struct {
	peer string
	id   uint32
}

// session — состояние приёма одного потока UDPR. Порядок и сборка потока
// живут в domain.Reassembler; здесь остаются политика ACK и учёт времени.
type session struct {
	id   uint32
	peer port.Addr
	sink io.Writer

	asm      *domain.Reassembler
	finSeq   uint32
	haveFin  bool
	complete bool

	sinceAck   int
	ackPending bool
	lastAck    time.Time
	lastSeen   time.Time
	started    time.Time
	stats      ReceiverStats

	window   int
	ackEvery int
	ackDelay time.Duration
	clock    port.Clock
	send     func(*domain.Packet, port.Addr)
}

func newSession(id uint32, peer port.Addr, sink io.Writer, cfg ServerConfig,
	clock port.Clock, send func(*domain.Packet, port.Addr)) *session {
	now := clock.Now()
	return &session{
		id: id, peer: peer, sink: sink,
		asm:      domain.NewReassembler(),
		lastSeen: now, started: now, lastAck: now,
		window: cfg.Window, ackEvery: cfg.AckEvery, ackDelay: cfg.AckDelay,
		clock: clock, send: send,
	}
}

// sendAck отправляет ACK немедленно и сбрасывает состояние отложенного ACK.
func (s *session) sendAck() {
	s.sinceAck = 0
	s.ackPending = false
	s.lastAck = s.clock.Now()
	s.stats.Acks++
	s.send(&domain.Packet{
		Type:      domain.TypeAck,
		Session:   s.id,
		Window:    uint16(s.window),
		AckBase:   s.asm.Expected(),
		AckBitmap: s.asm.Bitmap(),
	}, s.peer)
}

// scheduleAck решает, слать ли ACK сейчас. Подтверждение на каждый пакет
// перегружает обратный канал и упирается в rate limit ACK Guard, поэтому ACK
// агрегируется: раз в ackEvery пакетов, но не реже ackDelay. Разрыв
// последовательности подтверждается немедленно — от этого зависит скорость
// повторной отправки.
func (s *session) scheduleAck(gap bool) {
	s.sinceAck++
	s.ackPending = true
	if gap || s.sinceAck >= s.ackEvery || s.clock.Now().Sub(s.lastAck) >= s.ackDelay {
		s.sendAck()
	}
}

// flushAck отправляет отложенный ACK, если он есть.
func (s *session) flushAck() {
	if s.ackPending {
		s.sendAck()
	}
}

// handle обрабатывает один пакет сессии. Возвращает true, когда поток завершён.
func (s *session) handle(p *domain.Packet) (bool, error) {
	s.lastSeen = s.clock.Now()
	switch p.Type {
	case domain.TypeHello:
		s.sendAck()
	case domain.TypeData:
		if err := s.onData(p); err != nil {
			return false, err
		}
	case domain.TypeFin:
		s.finSeq, s.haveFin = p.Seq, true
		s.sendAck()
	default: // ACK в прямом канале — не наш случай
		s.stats.Rejected++
	}
	if s.haveFin && s.asm.Expected() >= s.finSeq {
		s.flushAck()
		s.complete = true
		return true, nil
	}
	return false, nil
}

func (s *session) onData(p *domain.Packet) error {
	switch s.asm.Accept(p.Seq, p.Payload) {
	case domain.Duplicate:
		// дубликат: подтверждаем немедленно, отправитель явно не видел наш ACK
		s.stats.Dup++
		s.sendAck()
	case domain.OutOfWindow:
		// вне окна приёма: не подтверждаем, отправитель повторит позже
		s.stats.Rejected++
	case domain.Accepted:
		if !s.asm.InOrder(p.Seq) {
			s.stats.OutOfOrder++
		}
		s.stats.Received++
		s.stats.Bytes += uint64(len(p.Payload))
		for _, chunk := range s.asm.Drain() {
			if _, err := s.sink.Write(chunk); err != nil {
				return err
			}
		}
		// дырка в потоке — подтверждаем сразу, чтобы отправитель увидел её
		s.scheduleAck(s.asm.HasGap())
	}
	return nil
}
