package usecase

import (
	"bytes"
	"context"
	"crypto/sha256"
	"io"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/argus/udpr/internal/adapter/sysclock"
	"github.com/argus/udpr/internal/domain"
	"github.com/argus/udpr/internal/port"
)

// collector собирает потоки всех сессий сервера.
type collector struct {
	mu      sync.Mutex
	streams map[uint32]*bytes.Buffer
	ended   int
}

func newCollector() *collector {
	return &collector{streams: map[uint32]*bytes.Buffer{}}
}

func (c *collector) factory(info port.SessionInfo) (io.WriteCloser, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	buf := &bytes.Buffer{}
	c.streams[info.Session] = buf
	return nopCloser{buf}, nil
}

func (c *collector) onEnd(port.SessionInfo, ReceiverStats, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ended++
}

func (c *collector) get(session uint32) []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	if b, ok := c.streams[session]; ok {
		return b.Bytes()
	}
	return nil
}

func (c *collector) endedCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ended
}

// startServer поднимает сервер на loopback. Собственный OnSessionEnd теста
// сохраняется — счётчик коллектора довешивается к нему.
//
// Возвращённая функция останова дожидается выхода Serve: иначе горутина
// пережила бы тест, а её t.Errorf после завершения теста — паника рантайма
// вместо отчёта об ошибке.
func startServer(t *testing.T, cfg ServerConfig, c *collector, loss float64) (*Server, func()) {
	t.Helper()
	prev := cfg.OnSessionEnd
	cfg.OnSessionEnd = func(info port.SessionInfo, st ReceiverStats, ok bool) {
		if prev != nil {
			prev(info, st, ok)
		}
		c.onEnd(info, st, ok)
	}
	srv := NewServer(listen(t, loss), sysclock.New(), cfg, c.factory)
	ctx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup
	wg.Add(1)
	errc := make(chan error, 1)
	go func() {
		defer wg.Done()
		if err := srv.Serve(ctx); err != nil {
			errc <- err
		}
	}()

	return srv, func() {
		cancel()
		wg.Wait()
		select {
		case err := <-errc:
			t.Errorf("Serve вернул ошибку: %v", err)
		default:
		}
	}
}

func sendPayload(t *testing.T, addr string, session uint32, payload []byte) {
	t.Helper()
	cfg := DefaultSenderConfig()
	cfg.Session, cfg.RTO = session, 40*time.Millisecond
	s := newSender(t, dial(t, addr, "", 0), cfg)
	if _, err := s.SendStream(t.Context(), bytes.NewReader(payload)); err != nil {
		t.Fatalf("отправка сессии %08x не удалась: %v", session, err)
	}
}

func randomBytes(n int, seed int64) []byte {
	b := make([]byte, n)
	rand.New(rand.NewSource(seed)).Read(b)
	return b
}

func waitFor(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal(msg)
}

// Сервер должен принимать сессии одну за другой и не завершаться после FIN.
func TestServerSequentialSessions(t *testing.T) {
	c := newCollector()
	srv, stop := startServer(t, DefaultServerConfig(), c, 0)
	defer stop()

	want := map[uint32][]byte{}
	for i := uint32(1); i <= 3; i++ {
		payload := randomBytes(32*1024, int64(i))
		want[i] = payload
		sendPayload(t, srv.LocalAddr().String(), i, payload)
	}
	waitFor(t, func() bool { return c.endedCount() == 3 }, "сервер не закрыл три сессии")

	for id, payload := range want {
		if sha256.Sum256(c.get(id)) != sha256.Sum256(payload) {
			t.Errorf("сессия %d: данные не совпали", id)
		}
	}
	if tot := srv.Totals(); tot.Completed != 3 || tot.Sessions != 3 {
		t.Fatalf("итоги сервера: %s, ожидалось 3 завершённых сессии", tot)
	}
}

// Несколько источников одновременно — потоки не должны перемешиваться.
func TestServerConcurrentSessions(t *testing.T) {
	c := newCollector()
	cfg := DefaultServerConfig()
	srv, stop := startServer(t, cfg, c, 0.05)
	defer stop()

	want := map[uint32][]byte{}
	var wg sync.WaitGroup
	for i := uint32(10); i < 14; i++ {
		payload := randomBytes(64*1024, int64(i))
		want[i] = payload
		wg.Add(1)
		go func(id uint32, p []byte) {
			defer wg.Done()
			sendPayload(t, srv.LocalAddr().String(), id, p)
		}(i, payload)
	}
	wg.Wait()
	waitFor(t, func() bool { return c.endedCount() == 4 }, "сервер не закрыл четыре сессии")

	for id, payload := range want {
		if sha256.Sum256(c.get(id)) != sha256.Sum256(payload) {
			t.Errorf("сессия %d: данные перемешаны или потеряны", id)
		}
	}
}

// Замолчавшая сессия должна выселяться и считаться незавершённой:
// молча потерянный поток — худший отказ для дата-диода.
func TestServerEvictsIdleSession(t *testing.T) {
	c := newCollector()
	cfg := DefaultServerConfig()
	cfg.SessionIdle = 200 * time.Millisecond
	var completed []bool
	var mu sync.Mutex
	cfg.OnSessionEnd = func(_ port.SessionInfo, _ ReceiverStats, ok bool) {
		mu.Lock()
		completed = append(completed, ok)
		mu.Unlock()
	}
	srv, stop := startServer(t, cfg, c, 0)
	defer stop()

	// одиночный DATA без FIN — сессия открывается и повисает
	link := dial(t, srv.LocalAddr().String(), "", 0)
	wire, _ := (&domain.Packet{Type: domain.TypeData, Session: 77, Seq: 0,
		Payload: []byte("частичный поток")}).Encode()
	link.Send(wire, nil)

	waitFor(t, func() bool { return c.endedCount() == 1 }, "сессия не выселена по таймауту")
	mu.Lock()
	defer mu.Unlock()
	if completed[0] {
		t.Fatal("выселенная сессия помечена как успешно завершённая")
	}
	if tot := srv.Totals(); tot.Evicted != 1 {
		t.Fatalf("итоги сервера: %s, ожидалась 1 выселенная сессия", tot)
	}
}

// После завершения сессии сервер обязан продолжать работу.
func TestServerKeepsListeningAfterSession(t *testing.T) {
	c := newCollector()
	srv, stop := startServer(t, DefaultServerConfig(), c, 0)
	defer stop()

	sendPayload(t, srv.LocalAddr().String(), 1, randomBytes(4096, 1))
	waitFor(t, func() bool { return c.endedCount() == 1 }, "первая сессия не закрыта")

	time.Sleep(300 * time.Millisecond) // сервер простаивает

	second := randomBytes(4096, 2)
	sendPayload(t, srv.LocalAddr().String(), 2, second)
	waitFor(t, func() bool { return c.endedCount() == 2 }, "вторая сессия не принята")
	if !bytes.Equal(c.get(2), second) {
		t.Fatal("вторая сессия принята с искажениями")
	}
}

// Serve завершается по отмене контекста, а не сам по себе.
func TestServerStopsOnContext(t *testing.T) {
	c := newCollector()
	srv := NewServer(listen(t, 0), sysclock.New(), DefaultServerConfig(), c.factory)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx) }()

	select {
	case err := <-done:
		t.Fatalf("Serve вышел без отмены контекста: %v", err)
	case <-time.After(500 * time.Millisecond):
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Serve вернул ошибку при остановке: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Serve не завершился после отмены контекста")
	}
}
