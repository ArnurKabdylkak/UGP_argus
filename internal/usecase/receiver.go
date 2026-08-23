package usecase

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
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

	// Logger принимает диагностику. nil — молчать.
	Logger *slog.Logger
}

// Validate проверяет значения, заданные оператором, теми же правилами, что и
// постоянный приёмник: ноль — умолчание, выход за пределы формата — ошибка.
func (c ReceiverConfig) Validate() error {
	if c.IdleTimeout < 0 {
		return fmt.Errorf("idle=%s отрицателен", c.IdleTimeout)
	}
	return serverConfig(c).Validate()
}

// DefaultReceiverConfig — значения по умолчанию для одноразового приёма.
func DefaultReceiverConfig() ReceiverConfig {
	return ReceiverConfig{Window: 32, IdleTimeout: 10 * time.Second,
		AckEvery: 8, AckDelay: 2 * time.Millisecond}
}

// watchdogInterval — период опроса сторожа тишины. Нижний предел обязателен:
// idle короче четырёх наносекунд дал бы нулевой интервал, а NewTicker на нём
// паникует — процесс падал бы от значения флага.
func watchdogInterval(idle time.Duration) time.Duration {
	if step := idle / 4; step >= time.Millisecond {
		return step
	}
	return time.Millisecond
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
		Logger:        cfg.Logger,
	}
}

// nopCloser отдаёт writer вызывающей стороны как приёмник сессии: закрывать
// чужой поток одноразовый приём не вправе.
type nopCloser struct{ io.Writer }

func (nopCloser) Close() error { return nil }

// Receiver принимает ровно один поток UDPR и завершается по FIN или по тишине
// длиннее IdleTimeout. Это тонкая обёртка над Server: логика последовательности,
// reorder-буфера и политики ACK общая.
//
// ReceiveStream рассчитан на один вызов и не допускает параллельных: приёмник
// данных и счётчики принадлежат текущему потоку. Stats читать из другой
// горутины можно — счётчики под мьютексом.
type Receiver struct {
	srv  *Server
	idle time.Duration
	out  io.Writer

	// счётчики обновляются в горутине Serve, а читаются снаружи (мониторинг,
	// тесты), поэтому под мьютексом; там же живёт отмена текущего приёма
	mu     sync.Mutex
	stats  ReceiverStats
	cancel context.CancelFunc
}

// NewReceiver собирает одноразовый приёмник поверх канала и часов.
func NewReceiver(link port.Link, clock port.Clock, cfg ReceiverConfig) *Receiver {
	r := &Receiver{idle: normalizeIdle(cfg.IdleTimeout)}
	srvCfg := serverConfig(cfg)
	srvCfg.OnSessionEnd = r.onSessionEnd
	r.srv = NewServer(link, clock, srvCfg, r.sink)
	return r
}

// onSessionEnd закрывает приём: одноразовый приёмник живёт ровно одну сессию.
func (r *Receiver) onSessionEnd(_ port.SessionInfo, st ReceiverStats, _ bool) {
	r.mu.Lock()
	r.stats = st
	cancel := r.cancel
	r.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// Close закрывает канал.
func (r *Receiver) Close() error { return r.srv.Close() }

// LocalAddr возвращает адрес прослушивания.
func (r *Receiver) LocalAddr() port.Addr { return r.srv.LocalAddr() }

// Stats возвращает счётчики принятого потока. Безопасно вызывать из другой
// горутины, в том числе во время приёма: до завершения сессии счётчики нулевые.
func (r *Receiver) Stats() ReceiverStats {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.stats
}

// addRejected досчитывает пакеты, отброшенные до разбора сессии.
func (r *Receiver) addRejected(n uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.stats.Rejected += n
}

func (r *Receiver) sink(port.SessionInfo) (io.WriteCloser, error) {
	return nopCloser{r.out}, nil
}

// ReceiveStream принимает один поток и пишет доставленные по порядку данные в w.
// Приём заканчивается по FIN, по тишине длиннее IdleTimeout или по отмене ctx.
func (r *Receiver) ReceiveStream(ctx context.Context, w io.Writer) (ReceiverStats, error) {
	r.out = w
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	r.mu.Lock()
	r.cancel = cancel
	r.mu.Unlock()

	// сессия так и не началась — выходим по общей тишине
	go func() {
		t := time.NewTicker(watchdogInterval(r.idle))
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
	r.addRejected(r.srv.Totals().Rejected)
	return r.Stats(), err
}
