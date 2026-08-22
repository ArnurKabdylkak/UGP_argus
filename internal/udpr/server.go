package udpr

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	mrand "math/rand"
	"net"
	"sync"
	"time"
)

// SessionInfo описывает поток, для которого запрашивается приёмник данных.
type SessionInfo struct {
	Session uint32
	Peer    *net.UDPAddr
	Started time.Time
}

func (i SessionInfo) String() string {
	return fmt.Sprintf("%08x от %s", i.Session, i.Peer)
}

// SinkFactory выдаёт приёмник данных под новую сессию. Возвращённый
// io.WriteCloser закрывается при завершении сессии.
type SinkFactory func(SessionInfo) (io.WriteCloser, error)

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
	Loss          float64 // имитация потерь обратного канала, 0..1
	Verbose       bool

	// OnSessionEnd вызывается при завершении или выселении сессии.
	OnSessionEnd func(SessionInfo, ReceiverStats, bool)
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

func (c *ServerConfig) normalize() {
	if c.Window <= 0 || c.Window > BitmapBits {
		c.Window = BitmapBits
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
}

// Server — постоянно слушающий приёмник UDPR. Обслуживает несколько сессий
// одновременно, переживает завершение и переустановку потоков и не выходит
// сам: работает, пока не отменён контекст.
type Server struct {
	cfg     ServerConfig
	conn    *net.UDPConn
	factory SinkFactory
	rng     *mrand.Rand

	sessions map[sessionKey]*session
	sinks    map[sessionKey]io.Closer

	// счётчики читаются снаружи (мониторинг, тесты), поэтому под мьютексом;
	// сами сессии живут в одной горутине Serve и блокировки не требуют
	mu    sync.Mutex
	total Totals
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

// NewServer открывает сокет и готовит постоянный приём.
func NewServer(bind string, cfg ServerConfig, factory SinkFactory) (*Server, error) {
	addr, err := net.ResolveUDPAddr("udp", bind)
	if err != nil {
		return nil, err
	}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return nil, err
	}
	return NewServerConn(conn, cfg, factory), nil
}

// NewServerConn собирает сервер поверх готового сокета (для тестов).
func NewServerConn(conn *net.UDPConn, cfg ServerConfig, factory SinkFactory) *Server {
	cfg.normalize()
	return &Server{
		cfg:      cfg,
		conn:     conn,
		factory:  factory,
		rng:      mrand.New(mrand.NewSource(time.Now().UnixNano())),
		sessions: make(map[sessionKey]*session),
		sinks:    make(map[sessionKey]io.Closer),
	}
}

// Close закрывает сокет.
func (s *Server) Close() error { return s.conn.Close() }

// LocalAddr возвращает адрес прослушивания.
func (s *Server) LocalAddr() net.Addr { return s.conn.LocalAddr() }

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

func (s *Server) logf(format string, a ...any) {
	if s.cfg.Verbose {
		log.Printf("[server] "+format, a...)
	}
}

func (s *Server) sendPacket(p *Packet, dst *net.UDPAddr) {
	if s.cfg.Loss > 0 && s.rng.Float64() < s.cfg.Loss {
		return
	}
	wire, err := p.Encode()
	if err != nil {
		return
	}
	if _, err := s.conn.WriteToUDP(wire, dst); err != nil {
		s.logf("ошибка отправки ACK: %v", err)
	}
}

// Serve принимает потоки, пока не отменён ctx. Возврат nil означает штатную
// остановку по контексту.
func (s *Server) Serve(ctx context.Context) error {
	defer s.closeAll()
	buf := make([]byte, 65535)

	for {
		if err := ctx.Err(); err != nil {
			return nil
		}

		// пока есть отложенные ACK, просыпаемся часто, чтобы их сбросить
		wait := 200 * time.Millisecond
		if s.hasPendingAcks() {
			wait = s.cfg.AckDelay
		}
		_ = s.conn.SetReadDeadline(time.Now().Add(wait))

		n, src, err := s.conn.ReadFromUDP(buf)
		if err != nil {
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				s.flushAcks()
				s.evictIdle()
				continue
			}
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}

		p, decErr := Decode(buf[:n])
		if decErr != nil {
			s.bump(func(t *Totals) { t.Rejected++ })
			s.logf("отброшен пакет от %s: %v", src, decErr)
			continue
		}
		if err := s.dispatch(p, src); err != nil {
			s.logf("сессия %08x: %v", p.Session, err)
		}
		s.evictIdle()
	}
}

func (s *Server) dispatch(p *Packet, src *net.UDPAddr) error {
	key := sessionKey{peer: src.String(), id: p.Session}
	sess, ok := s.sessions[key]
	if !ok {
		// новую сессию открывают только HELLO и DATA; ACK и FIN от неизвестного
		// потока — это мусор или повтор уже закрытой сессии
		if p.Type != TypeHello && p.Type != TypeData {
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
		info := SessionInfo{Session: p.Session, Peer: src, Started: time.Now()}
		sink, err := s.factory(info)
		if err != nil {
			s.bump(func(t *Totals) { t.Rejected++ })
			return fmt.Errorf("не удалось открыть приёмник: %w", err)
		}
		sess = newSession(p.Session, src, sink, s.cfg, s.sendPacket)
		s.sessions[key] = sess
		s.sinks[key] = sink
		s.bump(func(t *Totals) { t.Sessions++ })
		s.logf("новая сессия %s", info)
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
	s.logf("сессия %08x завершена (%s), %s", sess.id,
		map[bool]string{true: "поток закрыт", false: "прервана"}[completed], sess.stats)
	if s.cfg.OnSessionEnd != nil {
		s.cfg.OnSessionEnd(SessionInfo{Session: sess.id, Peer: sess.peer, Started: sess.started},
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
	for key, sess := range s.sessions {
		if time.Since(sess.lastSeen) > s.cfg.SessionIdle {
			s.logf("сессия %08x выселена по таймауту, доставлено %d байт (не завершена)",
				sess.id, sess.stats.Bytes)
			s.finish(key, sess, false)
		}
	}
}

func (s *Server) closeAll() {
	for key, sess := range s.sessions {
		s.finish(key, sess, sess.complete)
	}
}
