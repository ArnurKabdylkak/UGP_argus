package udpr

import (
	"io"
	"net"
	"time"
)

// sessionKey различает потоки: один и тот же session_id от разных источников —
// это разные сессии.
type sessionKey struct {
	peer string
	id   uint32
}

// session — состояние приёма одного потока UDPR.
type session struct {
	id   uint32
	peer *net.UDPAddr
	sink io.Writer

	expected uint32 // первый ещё не полученный seq
	buffer   map[uint32][]byte
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
	send     func(*Packet, *net.UDPAddr)
}

func newSession(id uint32, peer *net.UDPAddr, sink io.Writer, cfg ServerConfig,
	send func(*Packet, *net.UDPAddr)) *session {
	now := time.Now()
	return &session{
		id: id, peer: peer, sink: sink,
		buffer:   make(map[uint32][]byte),
		lastSeen: now, started: now, lastAck: now,
		window: cfg.Window, ackEvery: cfg.AckEvery, ackDelay: cfg.AckDelay,
		send: send,
	}
}

// sendAck отправляет ACK немедленно и сбрасывает состояние отложенного ACK.
func (s *session) sendAck() {
	s.sinceAck = 0
	s.ackPending = false
	s.lastAck = time.Now()
	s.stats.Acks++
	s.send(&Packet{
		Type:      TypeAck,
		Session:   s.id,
		Window:    uint16(s.window),
		AckBase:   s.expected,
		AckBitmap: s.bitmap(),
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
	if gap || s.sinceAck >= s.ackEvery || time.Since(s.lastAck) >= s.ackDelay {
		s.sendAck()
	}
}

// flushAck отправляет отложенный ACK, если он есть.
func (s *session) flushAck() {
	if s.ackPending {
		s.sendAck()
	}
}

func (s *session) bitmap() uint32 {
	var bm uint32
	for i := 0; i < BitmapBits; i++ {
		if _, ok := s.buffer[s.expected+1+uint32(i)]; ok {
			bm |= 1 << uint(i)
		}
	}
	return bm
}

// handle обрабатывает один пакет сессии. Возвращает true, когда поток завершён.
func (s *session) handle(p *Packet) (bool, error) {
	s.lastSeen = time.Now()
	switch p.Type {
	case TypeHello:
		s.sendAck()
	case TypeData:
		if err := s.onData(p); err != nil {
			return false, err
		}
	case TypeFin:
		s.finSeq, s.haveFin = p.Seq, true
		s.sendAck()
	default: // ACK в прямом канале — не наш случай
		s.stats.Rejected++
	}
	if s.haveFin && s.expected >= s.finSeq {
		s.flushAck()
		s.complete = true
		return true, nil
	}
	return false, nil
}

func (s *session) onData(p *Packet) error {
	_, dup := s.buffer[p.Seq]
	switch {
	case p.Seq < s.expected || dup:
		// дубликат: подтверждаем немедленно, отправитель явно не видел наш ACK
		s.stats.Dup++
		s.sendAck()
	case p.Seq >= s.expected+1+BitmapBits:
		// вне окна приёма: не подтверждаем, отправитель повторит позже
		s.stats.Rejected++
	default:
		if p.Seq != s.expected {
			s.stats.OutOfOrder++
		}
		s.buffer[p.Seq] = p.Payload
		s.stats.Received++
		s.stats.Bytes += uint64(len(p.Payload))
		if err := s.deliver(); err != nil {
			return err
		}
		// дырка в потоке — подтверждаем сразу, чтобы отправитель увидел её
		s.scheduleAck(len(s.buffer) > 0)
	}
	return nil
}

// deliver отдаёт наверх непрерывный префикс потока.
func (s *session) deliver() error {
	for {
		data, ok := s.buffer[s.expected]
		if !ok {
			return nil
		}
		delete(s.buffer, s.expected)
		if _, err := s.sink.Write(data); err != nil {
			return err
		}
		s.expected++
	}
}
