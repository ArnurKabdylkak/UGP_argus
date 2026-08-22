package usecase

import (
	"context"
	"io"
	"time"

	"github.com/argus/udpr/internal/port"
)

// ReceiverConfig — параметры одноразового приёма (команда recv): принять один
// поток и выйти. Для постоянно работающего шлюза используйте Server.
type ReceiverConfig struct {
	Window      int           // окно приёма в пакетах (<= BitmapBits)
	IdleTimeout time.Duration // сколько ждать тишины до выхода
	AckEvery    int           // ACK не чаще одного на N принятых пакетов (0 — window/4)
	AckDelay    time.Duration // предел задержки отложенного ACK
	Verbose     bool
}

// DefaultReceiverConfig — значения по умолчанию для одноразового приёма.
func DefaultReceiverConfig() ReceiverConfig {
	return ReceiverConfig{Window: 32, IdleTimeout: 10 * time.Second,
		AckEvery: 8, AckDelay: 2 * time.Millisecond}
}

func normalizeIdle(d time.Duration) time.Duration {
	if d <= 0 {
		return 10 * time.Second
	}
	return d
}

func serverConfig(cfg ReceiverConfig) ServerConfig {
	return ServerConfig{
		Window:        cfg.Window,
		AckEvery:      cfg.AckEvery,
		AckDelay:      cfg.AckDelay,
		SessionIdle:   normalizeIdle(cfg.IdleTimeout),
		MaxSessions:   1,
		SingleSession: true,
		Verbose:       cfg.Verbose,
	}
}

// nopCloser отдаёт writer вызывающей стороны как приёмник сессии: закрывать
// чужой поток одноразовый приём не вправе.
type nopCloser struct{ io.Writer }

func (nopCloser) Close() error { return nil }

// Receiver принимает ровно один поток UDPR и завершается по FIN или по тишине
// длиннее IdleTimeout. Это тонкая обёртка над Server: логика последовательности,
// reorder-буфера и политики ACK общая.
type Receiver struct {
	srv   *Server
	idle  time.Duration
	out   io.Writer
	stats ReceiverStats
}

// NewReceiver собирает одноразовый приёмник поверх канала и часов.
func NewReceiver(link port.Link, clock port.Clock, cfg ReceiverConfig) *Receiver {
	r := &Receiver{idle: normalizeIdle(cfg.IdleTimeout)}
	r.srv = NewServer(link, clock, serverConfig(cfg), r.sink)
	return r
}

// Close закрывает канал.
func (r *Receiver) Close() error { return r.srv.Close() }

// LocalAddr возвращает адрес прослушивания.
func (r *Receiver) LocalAddr() port.Addr { return r.srv.LocalAddr() }

// Stats возвращает счётчики принятого потока.
func (r *Receiver) Stats() ReceiverStats { return r.stats }

func (r *Receiver) sink(port.SessionInfo) (io.WriteCloser, error) {
	return nopCloser{r.out}, nil
}

// ReceiveStream принимает один поток и пишет доставленные по порядку данные в w.
func (r *Receiver) ReceiveStream(w io.Writer) (ReceiverStats, error) {
	r.out = w
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	r.srv.cfg.OnSessionEnd = func(_ port.SessionInfo, st ReceiverStats, _ bool) {
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
