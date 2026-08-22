// Package usecase — прикладной слой UDPR: сценарии передачи и приёма потока.
// Опирается только на domain и port; конкретных сетей и файлов не знает.
//
// Сборка сценариев с конкретными адаптерами живёт в internal/app.
package usecase

import (
	"crypto/rand"
	"fmt"
	"io"
	"log"
	"math/big"
	"time"

	"github.com/argus/udpr/internal/domain"
	"github.com/argus/udpr/internal/port"
)

// SenderConfig — параметры отправителя.
type SenderConfig struct {
	MTU        int           // payload на пакет, байт
	Window     int           // размер окна в пакетах (<= BitmapBits)
	RTO        time.Duration // таймаут повторной отправки
	MaxRetries int           // предел попыток на пакет
	AckRate    float64       // предел частоты ACK, шт/с
	AckBurst   float64       // допустимый всплеск ACK
	Session    uint32        // 0 — сгенерировать случайно
	Verbose    bool
}

// DefaultSenderConfig — разумные значения по умолчанию для MVP.
func DefaultSenderConfig() SenderConfig {
	return SenderConfig{
		MTU:        domain.MaxPayload,
		Window:     32,
		RTO:        250 * time.Millisecond,
		MaxRetries: 20,
		// приёмник агрегирует ACK, поэтому легальная частота — это pps/AckEvery
		// плюс немедленные ACK на разрывы; 20k/с с запасом покрывает 10 Гбит/с
		AckRate:  20000,
		AckBurst: 1024,
	}
}

func (c *SenderConfig) normalize() {
	if c.MTU <= 0 || c.MTU > domain.MaxPayload {
		c.MTU = domain.MaxPayload
	}
	if c.Window <= 0 || c.Window > domain.BitmapBits {
		c.Window = domain.BitmapBits
	}
	if c.RTO <= 0 {
		c.RTO = 250 * time.Millisecond
	}
	if c.MaxRetries <= 0 {
		c.MaxRetries = 20
	}
	if c.AckRate <= 0 {
		c.AckRate = 20000
	}
	if c.AckBurst <= 0 {
		c.AckBurst = 1024
	}
	if c.Session == 0 {
		c.Session = randomSession()
	}
}

// SenderStats — счётчики по итогам передачи.
type SenderStats struct {
	Sent       uint64
	Retransmit uint64
	Acks       uint64
	Bytes      uint64
}

func (s SenderStats) String() string {
	return fmt.Sprintf("sent=%d retransmit=%d acks=%d bytes=%d",
		s.Sent, s.Retransmit, s.Acks, s.Bytes)
}

// Sender передаёт поток байт по UDPR: буферизует пакеты до подтверждения,
// следит за таймаутами и выполняет повторную отправку.
type Sender struct {
	cfg   SenderConfig
	link  port.Link
	clock port.Clock
	guard *domain.AckGuard
	win   *domain.Window
	stats SenderStats
}

// NewSender собирает отправителя поверх канала и часов.
func NewSender(link port.Link, clock port.Clock, cfg SenderConfig) *Sender {
	cfg.normalize()
	return &Sender{
		cfg:   cfg,
		link:  link,
		clock: clock,
		guard: domain.NewAckGuard(cfg.Session, cfg.AckRate, cfg.AckBurst),
		win:   domain.NewWindow(cfg.Window),
	}
}

// Session возвращает идентификатор сессии.
func (s *Sender) Session() uint32 { return s.cfg.Session }

// Guard даёт доступ к политике обратного канала.
func (s *Sender) Guard() *domain.AckGuard { return s.guard }

// LocalAddr — адрес, на который придут подтверждения.
func (s *Sender) LocalAddr() port.Addr { return s.link.LocalAddr() }

func (s *Sender) logf(format string, a ...any) {
	if s.cfg.Verbose {
		log.Printf("[sender] "+format, a...)
	}
}

// emit отправляет пакет окна и учитывает попытку.
func (s *Sender) emit(seq uint32) {
	wire, ok := s.win.Wire(seq)
	if !ok {
		return
	}
	tries := s.win.MarkSent(seq, s.clock.Now())
	if tries > 1 {
		s.stats.Retransmit++
		s.logf("retransmit seq=%d try=%d", seq, tries)
	}
	if err := s.link.Send(wire, nil); err != nil {
		s.logf("ошибка отправки: %v", err)
	}
}

func (s *Sender) sendControl(p *domain.Packet) {
	wire, err := p.Encode()
	if err != nil {
		return
	}
	if err := s.link.Send(wire, nil); err != nil {
		s.logf("ошибка отправки %s: %v", domain.TypeName(p.Type), err)
	}
}

// SendStream заворачивает поток r в пакеты UDPR и доставляет их с гарантией.
func (s *Sender) SendStream(r io.Reader) (SenderStats, error) {
	s.sendControl(&domain.Packet{
		Type: domain.TypeHello, Session: s.cfg.Session, Window: uint16(s.cfg.Window),
	})

	buf := make([]byte, s.cfg.MTU)
	eof := false
	for !eof || !s.win.Empty() {
		for !eof && s.win.CanQueue() {
			n, err := io.ReadFull(r, buf)
			if n > 0 {
				if err := s.queue(buf[:n]); err != nil {
					return s.stats, err
				}
			}
			if err != nil { // EOF или ErrUnexpectedEOF — поток закончился
				eof = true
			}
		}
		if eof && s.win.Empty() {
			break
		}

		dg, err := s.link.Recv(s.cfg.RTO / 4)
		switch {
		case err == nil:
			s.applyDatagram(dg)
		case port.IsTimeout(err):
			// нечего разбирать, ниже отработают таймауты окна
		default:
			return s.stats, err
		}
		if err := s.checkTimeouts(); err != nil {
			return s.stats, err
		}
	}

	s.sendControl(&domain.Packet{
		Type: domain.TypeFin, Session: s.cfg.Session,
		Seq: s.win.Next(), Window: uint16(s.cfg.Window),
	})
	return s.stats, nil
}

func (s *Sender) queue(chunk []byte) error {
	seq := s.win.Next()
	p := &domain.Packet{
		Type: domain.TypeData, Session: s.cfg.Session, Seq: seq,
		Window: uint16(s.cfg.Window), Payload: chunk,
	}
	wire, err := p.Encode()
	if err != nil {
		return err
	}
	s.win.Queue(wire)
	s.stats.Sent++
	s.stats.Bytes += uint64(len(chunk))
	s.emit(seq)
	return nil
}

// applyDatagram разбирает пришедшее из обратного канала и применяет ACK.
func (s *Sender) applyDatagram(dg port.Datagram) {
	ack, err := domain.Decode(dg.Payload)
	if err != nil {
		s.guard.Drop(err.Error())
		s.logf("отброшен пакет: %v", err)
		return
	}
	now := s.clock.Now()
	if !s.guard.Check(ack, s.win.Base(), s.win.Next(), now) {
		_, reason := s.guard.Stats()
		s.logf("ACK Guard отклонил: %s", reason)
		return
	}
	s.stats.Acks++
	for _, seq := range s.win.Ack(ack.AckBase, ack.AckBitmap, now, s.cfg.RTO) {
		s.emit(seq)
	}
}

func (s *Sender) checkTimeouts() error {
	now := s.clock.Now()
	for _, seq := range s.win.Expired(now, s.cfg.RTO) {
		if s.win.Tries(seq) > s.cfg.MaxRetries {
			return fmt.Errorf("udpr: seq=%d не доставлен после %d попыток", seq, s.win.Tries(seq))
		}
		s.emit(seq)
	}
	return nil
}

func randomSession() uint32 {
	n, err := rand.Int(rand.Reader, big.NewInt(1<<32-1))
	if err != nil {
		return uint32(time.Now().UnixNano()) | 1
	}
	return uint32(n.Uint64()) | 1
}
