package usecase

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/argus/udpr/internal/domain"
	"github.com/argus/udpr/internal/port"
)

// ServerConfig — параметры постоянно работающего приёмника.
type ServerConfig struct {
	Window      int           // окно приёма в пакетах (<= BitmapBits)
	AckEvery    int           // ACK не чаще одного на N пакетов (0 — window/4)
	AckDelay    time.Duration // предел задержки отложенного ACK
	SessionIdle time.Duration // выселение сессии, замолчавшей на это время
	MaxSessions int           // предел одновременных сессий
	// SingleSession принимает ровно один поток и отбрасывает всё остальное —
	// режим одноразового приёма (команда recv).
	SingleSession bool

	// Logger принимает диагностику. nil — молчать.
	Logger *slog.Logger

	// OnSessionEnd вызывается при завершении или выселении сессии.
	OnSessionEnd func(port.SessionInfo, ReceiverStats, bool)
}

// Validate проверяет значения, заданные оператором. Ноль означает «взять
// умолчание»; выход за пределы формата — ошибка, а не повод молча подставить
// своё.
func (c ServerConfig) Validate() error {
	switch {
	case c.Window < 0 || c.Window > domain.BitmapBits:
		return fmt.Errorf("window=%d вне диапазона 1..%d", c.Window, domain.BitmapBits)
	case c.AckEvery < 0:
		return fmt.Errorf("ack-every=%d отрицателен", c.AckEvery)
	case c.AckDelay < 0:
		return fmt.Errorf("ack-delay=%s отрицателен", c.AckDelay)
	case c.SessionIdle < 0:
		return fmt.Errorf("idle=%s отрицателен", c.SessionIdle)
	case c.MaxSessions < 0:
		return fmt.Errorf("max=%d отрицателен", c.MaxSessions)
	}
	return nil
}

// DefaultServerConfig — значения по умолчанию для постоянного приёма.
func DefaultServerConfig() ServerConfig {
	return ServerConfig{
		Window:      32,
		AckEvery:    8,
		AckDelay:    2 * time.Millisecond,
		SessionIdle: 60 * time.Second,
		MaxSessions: 64,
	}
}

// normalize подставляет умолчания вместо нулей. Проверка допустимости — дело
// Validate на границе приложения.
func (c *ServerConfig) normalize() {
	if c.Window <= 0 || c.Window > domain.BitmapBits {
		c.Window = domain.BitmapBits
	}
	if c.AckEvery <= 0 {
		c.AckEvery = c.Window / 4
	}
	if c.AckEvery <= 0 {
		c.AckEvery = 1
	}
	if c.AckDelay <= 0 {
		c.AckDelay = 2 * time.Millisecond
	}
	if c.SessionIdle <= 0 {
		c.SessionIdle = 60 * time.Second
	}
	if c.MaxSessions <= 0 {
		c.MaxSessions = 64
	}
	if c.Logger == nil {
		c.Logger = slog.New(slog.DiscardHandler)
	}
}

// Totals — сводные счётчики сервера за всё время работы.
type Totals struct {
	Sessions  uint64
	Completed uint64
	Evicted   uint64
	Rejected  uint64 // пакеты, отброшенные до разбора сессии
	Bytes     uint64
}

func (t Totals) String() string {
	return fmt.Sprintf("sessions=%d completed=%d evicted=%d rejected=%d bytes=%d",
		t.Sessions, t.Completed, t.Evicted, t.Rejected, t.Bytes)
}

// Server — постоянно слушающий приёмник UDPR. Обслуживает несколько сессий
// одновременно, переживает завершение и переустановку потоков и не выходит
// сам: работает, пока не отменён контекст.
//
// Знает только port.Link и port.Clock: ни UDP, ни файлов, ни системных часов
// в этом слое нет.
type Server struct {
	cfg     ServerConfig
	log     *slog.Logger
	link    port.Link
	clock   port.Clock
	factory port.SinkFactory

	sessions map[sessionKey]*session
	sinks    map[sessionKey]io.Closer

	// буфер исходящих: сериализация живёт в горутине Serve, поэтому один
	// буфер на сервер вместо аллокации на каждый ACK
	wire []byte

	// счётчики читаются снаружи (мониторинг, тесты), поэтому под мьютексом;
	// сами сессии живут в одной горутине Serve и блокировки не требуют
	mu    sync.Mutex
	total Totals
}

// NewServer собирает приёмник поверх канала, часов и фабрики приёмников данных.
func NewServer(link port.Link, clock port.Clock, cfg ServerConfig, factory port.SinkFactory) *Server {
	cfg.normalize()
	return &Server{
		cfg:      cfg,
		log:      cfg.Logger.With("component", "server"),
		link:     link,
		clock:    clock,
		factory:  factory,
		sessions: make(map[sessionKey]*session, cfg.MaxSessions),
		sinks:    make(map[sessionKey]io.Closer, cfg.MaxSessions),
		wire:     make([]byte, 0, domain.HeaderLen),
	}
}

// Close закрывает канал.
func (s *Server) Close() error { return s.link.Close() }

// LocalAddr возвращает адрес прослушивания.
func (s *Server) LocalAddr() port.Addr { return s.link.LocalAddr() }

// Totals возвращает сводные счётчики. Безопасно вызывать из другой горутины.
func (s *Server) Totals() Totals {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.total
}

// bump атомарно обновляет счётчики сервера.
func (s *Server) bump(f func(*Totals)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	f(&s.total)
}

func (s *Server) sendPacket(p *domain.Packet, dst port.Addr) {
	wire, err := p.AppendTo(s.wire[:0])
	if err != nil {
		return
	}
	s.wire = wire
	if err := s.link.Send(wire, dst); err != nil {
		s.log.Debug("ошибка отправки ACK", "peer", dst, "err", err)
	}
}

// Serve принимает потоки, пока не отменён ctx. Возврат nil означает штатную
// остановку по контексту.
func (s *Server) Serve(ctx context.Context) error {
	defer s.closeAll()

	for {
		if err := ctx.Err(); err != nil {
			return nil
		}

		// пока есть отложенные ACK, просыпаемся часто, чтобы их сбросить
		wait := 200 * time.Millisecond
		if s.hasPendingAcks() {
			wait = s.cfg.AckDelay
		}

		dg, err := s.link.Recv(wait)
		switch {
		case err == nil:
		case port.IsTimeout(err):
			s.flushAcks()
			s.evictIdle()
			continue
		case ctx.Err() != nil || errors.Is(err, port.ErrClosed):
			return nil
		default:
			return err
		}

		p, decErr := domain.Decode(dg.Payload)
		if decErr != nil {
			s.bump(func(t *Totals) { t.Rejected++ })
			s.log.Debug("отброшен пакет", "peer", dg.Peer, "err", decErr)
			continue
		}
		if err := s.dispatch(p, dg.Peer); err != nil {
			s.log.Warn("сессия завершена с ошибкой", "session", p.Session, "err", err)
		}
		s.evictIdle()
	}
}

func (s *Server) dispatch(p *domain.Packet, src port.Addr) error {
	key := sessionKey{peer: src.String(), id: p.Session}
	sess, ok := s.sessions[key]
	if !ok {
		// новую сессию открывают только HELLO и DATA; ACK и FIN от неизвестного
		// потока — это мусор или повтор уже закрытой сессии
		if p.Type != domain.TypeHello && p.Type != domain.TypeData {
			s.bump(func(t *Totals) { t.Rejected++ })
			return nil
		}
		if s.cfg.SingleSession && s.Totals().Sessions > 0 {
			s.bump(func(t *Totals) { t.Rejected++ })
			return nil
		}
		if len(s.sessions) >= s.cfg.MaxSessions {
			s.bump(func(t *Totals) { t.Rejected++ })
			return fmt.Errorf("достигнут предел сессий (%d), поток от %s отклонён",
				s.cfg.MaxSessions, src)
		}
		info := port.SessionInfo{Session: p.Session, Peer: src, Started: s.clock.Now()}
		sink, err := s.factory(info)
		if err != nil {
			s.bump(func(t *Totals) { t.Rejected++ })
			return fmt.Errorf("не удалось открыть приёмник: %w", err)
		}
		sess = newSession(p.Session, src, sink, s.cfg, s.clock, s.sendPacket)
		s.sessions[key] = sess
		s.sinks[key] = sink
		s.bump(func(t *Totals) { t.Sessions++ })
		s.log.Info("новая сессия", "session", info.Session, "peer", info.Peer)
	}

	done, err := sess.handle(p)
	if err != nil {
		s.finish(key, sess, false)
		return err
	}
	if done {
		s.finish(key, sess, true)
	}
	return nil
}

func (s *Server) finish(key sessionKey, sess *session, completed bool) {
	if c, ok := s.sinks[key]; ok {
		c.Close()
		delete(s.sinks, key)
	}
	delete(s.sessions, key)
	s.bump(func(t *Totals) {
		t.Bytes += sess.stats.Bytes
		if completed {
			t.Completed++
		} else {
			t.Evicted++
		}
	})
	outcome := "прервана"
	if completed {
		outcome = "поток закрыт"
	}
	s.log.Info("сессия закрыта", "session", sess.id, "outcome", outcome,
		"bytes", sess.stats.Bytes, "stats", sess.stats)
	if s.cfg.OnSessionEnd != nil {
		s.cfg.OnSessionEnd(port.SessionInfo{Session: sess.id, Peer: sess.peer, Started: sess.started},
			sess.stats, completed)
	}
}

func (s *Server) hasPendingAcks() bool {
	for _, sess := range s.sessions {
		if sess.ackPending {
			return true
		}
	}
	return false
}

func (s *Server) flushAcks() {
	for _, sess := range s.sessions {
		sess.flushAck()
	}
}

// evictIdle закрывает сессии, замолчавшие дольше SessionIdle. Незавершённый
// поток — это потеря данных, поэтому такие сессии считаются отдельно.
func (s *Server) evictIdle() {
	now := s.clock.Now()
	for key, sess := range s.sessions {
		if now.Sub(sess.lastSeen) > s.cfg.SessionIdle {
			s.log.Warn("сессия выселена по таймауту (поток не завершён)",
				"session", sess.id, "bytes", sess.stats.Bytes)
			s.finish(key, sess, false)
		}
	}
}

func (s *Server) closeAll() {
	for key, sess := range s.sessions {
		s.finish(key, sess, sess.complete)
	}
}
