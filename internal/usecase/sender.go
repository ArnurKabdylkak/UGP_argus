// Package usecase — прикладной слой UDPR: сценарии передачи и приёма потока.
// Опирается только на domain и port; конкретных сетей и файлов не знает.
//
// Сборка сценариев с конкретными адаптерами живёт в internal/app.
package usecase

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"time"

	"github.com/argus/udpr/internal/domain"
	"github.com/argus/udpr/internal/port"
)

// ErrDeliveryFailed — поток не доставлен: пакет исчерпал попытки повтора.
// Для дата-диода это единственный отказ, ради которого стоит бить тревогу,
// поэтому он отличим от ошибок сокета и чтения источника.
var ErrDeliveryFailed = errors.New("udpr: поток не доставлен")

// SenderConfig — параметры отправителя.
type SenderConfig struct {
	MTU        int           // payload на пакет, байт
	Window     int           // размер окна в пакетах (<= BitmapBits)
	RTO        time.Duration // таймаут повторной отправки
	MaxRetries int           // предел попыток на пакет
	AckRate    float64       // предел частоты ACK, шт/с
	AckBurst   float64       // допустимый всплеск ACK
	Session    uint32        // 0 — сгенерировать случайно

	// Logger принимает диагностику. nil — молчать.
	Logger *slog.Logger
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

// Validate проверяет значения, заданные оператором. Ноль означает «взять
// умолчание» и допустим везде; выход за пределы формата — ошибка, а не повод
// молча подставить своё: канал настраивают под конкретное железо, и подмена
// MTU или окна за спиной оператора хуже отказа запуска.
func (c SenderConfig) Validate() error {
	switch {
	case c.MTU < 0 || c.MTU > domain.MaxPayload:
		return fmt.Errorf("mtu=%d вне диапазона 1..%d", c.MTU, domain.MaxPayload)
	case c.Window < 0 || c.Window > domain.BitmapBits:
		return fmt.Errorf("window=%d вне диапазона 1..%d", c.Window, domain.BitmapBits)
	case c.RTO < 0:
		return fmt.Errorf("rto=%s отрицателен", c.RTO)
	case c.MaxRetries < 0:
		return fmt.Errorf("max-retries=%d отрицателен", c.MaxRetries)
	case c.AckRate < 0 || c.AckBurst < 0:
		return fmt.Errorf("ack-rate=%.0f и ack-burst=%.0f должны быть неотрицательны",
			c.AckRate, c.AckBurst)
	}
	return nil
}

// normalize подставляет умолчания вместо нулей. Проверка допустимости — дело
// Validate на границе приложения.
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
	if c.Logger == nil {
		c.Logger = slog.New(slog.DiscardHandler)
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
	log   *slog.Logger
	link  port.Link
	clock port.Clock
	guard *domain.AckGuard
	win   *domain.Window
	stats SenderStats
}

// NewSender собирает отправителя поверх канала и часов. Ошибка возможна
// только при недоступном источнике случайности для идентификатора сессии:
// подменять его предсказуемым значением молча нельзя.
func NewSender(link port.Link, clock port.Clock, cfg SenderConfig) (*Sender, error) {
	cfg.normalize()
	if cfg.Session == 0 {
		session, err := randomSession()
		if err != nil {
			return nil, err
		}
		cfg.Session = session
	}
	return &Sender{
		cfg:   cfg,
		log:   cfg.Logger.With("component", "sender", "session", cfg.Session),
		link:  link,
		clock: clock,
		guard: domain.NewAckGuard(cfg.Session, cfg.AckRate, cfg.AckBurst),
		win:   domain.NewWindow(cfg.Window),
	}, nil
}

// Session возвращает идентификатор сессии.
func (s *Sender) Session() uint32 { return s.cfg.Session }

// Guard даёт доступ к политике обратного канала.
func (s *Sender) Guard() *domain.AckGuard { return s.guard }

// LocalAddr — адрес, на который придут подтверждения.
func (s *Sender) LocalAddr() port.Addr { return s.link.LocalAddr() }

// emit отправляет пакет окна и учитывает попытку.
func (s *Sender) emit(seq uint32) {
	wire, ok := s.win.Wire(seq)
	if !ok {
		return
	}
	tries := s.win.MarkSent(seq, s.clock.Now())
	if tries > 1 {
		s.stats.Retransmit++
		s.log.Debug("повторная отправка", "seq", seq, "try", tries)
	}
	if err := s.link.Send(wire, nil); err != nil {
		s.log.Debug("ошибка отправки", "seq", seq, "err", err)
	}
}

func (s *Sender) sendControl(p *domain.Packet) {
	wire, err := p.Encode()
	if err != nil {
		return
	}
	if err := s.link.Send(wire, nil); err != nil {
		s.log.Debug("ошибка отправки", "type", domain.TypeName(p.Type), "err", err)
	}
}

// SendStream заворачивает поток r в пакеты UDPR и доставляет их с гарантией.
//
// Отмена ctx прерывает передачу между итерациями цикла: повторы не должны
// продолжаться после того, как оператор нажал Ctrl-C или истёк общий дедлайн.
// Возвращается ctx.Err(), уже отправленное остаётся отправленным.
func (s *Sender) SendStream(ctx context.Context, r io.Reader) (SenderStats, error) {
	s.sendControl(&domain.Packet{
		Type: domain.TypeHello, Session: s.cfg.Session, Window: uint16(s.cfg.Window),
	})

	buf := make([]byte, s.cfg.MTU)
	eof := false
	for !eof || !s.win.Empty() {
		if err := ctx.Err(); err != nil {
			return s.stats, err
		}
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
		s.log.Debug("отброшен пакет обратного канала", "err", err)
		return
	}
	now := s.clock.Now()
	if !s.guard.Check(ack, s.win.Base(), s.win.Next(), now) {
		_, reason := s.guard.Stats()
		s.log.Debug("ACK Guard отклонил пакет", "reason", reason)
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
			return fmt.Errorf("%w: seq=%d исчерпал %d попыток", ErrDeliveryFailed, seq, s.win.Tries(seq))
		}
		s.emit(seq)
	}
	return nil
}

// randomSession выдаёт идентификатор сессии. Младший бит выставлен, чтобы
// значение не оказалось нулём: ноль в конфигурации означает «сгенерировать».
func randomSession() (uint32, error) {
	n, err := rand.Int(rand.Reader, big.NewInt(1<<32-1))
	if err != nil {
		return 0, fmt.Errorf("udpr: нет источника случайности для session_id: %w", err)
	}
	return uint32(n.Uint64()) | 1, nil
}
