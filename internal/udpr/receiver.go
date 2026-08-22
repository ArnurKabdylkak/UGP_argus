package udpr

import (
	"context"
	"fmt"
	"io"
	"net"
	"time"
)

// ReceiverConfig — параметры одноразового приёма (команда recv): принять один
// поток и выйти. Для постоянно работающего шлюза используйте Server.
type ReceiverConfig struct {
	Window      int           // окно приёма в пакетах (<= BitmapBits)
	IdleTimeout time.Duration // сколько ждать тишины до выхода
	AckEvery    int           // ACK не чаще одного на N принятых пакетов (0 — window/4)
	AckDelay    time.Duration // предел задержки отложенного ACK
	Loss        float64       // имитация потерь обратного канала, 0..1
	Verbose     bool
}

// DefaultReceiverConfig — значения по умолчанию для одноразового приёма.
func DefaultReceiverConfig() ReceiverConfig {
	return ReceiverConfig{Window: 32, IdleTimeout: 10 * time.Second,
		AckEvery: 8, AckDelay: 2 * time.Millisecond}
}

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

// Receiver принимает ровно один поток UDPR и завершается по FIN или по тишине
// длиннее IdleTimeout. Это тонкая обёртка над Server: логика последовательности,
// reorder-буфера и политики ACK общая.
type Receiver struct {
	srv   *Server
	idle  time.Duration
	out   io.Writer
	stats ReceiverStats
}

func normalizeIdle(d time.Duration) time.Duration {
	if d <= 0 {
		return 10 * time.Second
	}
	return d
}

func serverConfig(cfg ReceiverConfig) ServerConfig {
	cfg.IdleTimeout = normalizeIdle(cfg.IdleTimeout)
	return ServerConfig{
		Window:        cfg.Window,
		AckEvery:      cfg.AckEvery,
		AckDelay:      cfg.AckDelay,
		SessionIdle:   cfg.IdleTimeout,
		MaxSessions:   1,
		SingleSession: true,
		Loss:          cfg.Loss,
		Verbose:       cfg.Verbose,
	}
}

// NewReceiver слушает UDP на bind (например "0.0.0.0:5555").
func NewReceiver(bind string, cfg ReceiverConfig) (*Receiver, error) {
	r := &Receiver{idle: normalizeIdle(cfg.IdleTimeout)}
	srv, err := NewServer(bind, serverConfig(cfg), r.sink)
	if err != nil {
		return nil, err
	}
	r.srv = srv
	return r, nil
}

// NewReceiverConn собирает приёмник поверх готового сокета (для тестов).
func NewReceiverConn(conn *net.UDPConn, cfg ReceiverConfig) *Receiver {
	r := &Receiver{idle: normalizeIdle(cfg.IdleTimeout)}
	r.srv = NewServerConn(conn, serverConfig(cfg), r.sink)
	return r
}

// Close закрывает сокет.
func (r *Receiver) Close() error { return r.srv.Close() }

// LocalAddr возвращает адрес прослушивания.
func (r *Receiver) LocalAddr() net.Addr { return r.srv.LocalAddr() }

// Stats возвращает счётчики принятого потока.
func (r *Receiver) Stats() ReceiverStats { return r.stats }

// writer подставляется на время ReceiveStream.
type nopCloser struct{ io.Writer }

func (nopCloser) Close() error { return nil }

func (r *Receiver) sink(SessionInfo) (io.WriteCloser, error) {
	return nopCloser{r.out}, nil
}

// ReceiveStream принимает один поток и пишет доставленные по порядку данные в w.
func (r *Receiver) ReceiveStream(w io.Writer) (ReceiverStats, error) {
	r.out = w
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	r.srv.cfg.OnSessionEnd = func(_ SessionInfo, st ReceiverStats, _ bool) {
		r.stats = st
		cancel()
	}

	// сессия так и не началась — выходим по общей тишине
	go func() {
		t := time.NewTicker(r.idle / 4)
		defer t.Stop()
		start := time.Now()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if r.srv.Totals().Sessions == 0 && time.Since(start) > r.idle {
					cancel()
					return
				}
			}
		}
	}()

	err := r.srv.Serve(ctx)
	r.stats.Rejected += r.srv.Totals().Rejected
	return r.stats, err
}
