package usecase

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/argus/udpr/internal/adapter/lossylink"
	"github.com/argus/udpr/internal/adapter/sysclock"
	"github.com/argus/udpr/internal/adapter/udplink"
	"github.com/argus/udpr/internal/domain"
	"github.com/argus/udpr/internal/port"
)

// listen открывает канал приёма на свободном порту loopback.
func listen(t *testing.T, loss float64) port.Link {
	t.Helper()
	link, err := udplink.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { link.Close() })
	return lossylink.Wrap(link, loss)
}

// dial открывает канал в сторону remote; local закрепляет исходящий порт.
func dial(t *testing.T, remote, local string, loss float64) port.Link {
	t.Helper()
	link, err := udplink.Dial(remote, local)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { link.Close() })
	return lossylink.Wrap(link, loss)
}

// newSender собирает отправителя поверх готового канала.
func newSender(t *testing.T, link port.Link, cfg SenderConfig) *Sender {
	t.Helper()
	s, err := NewSender(link, sysclock.New(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// runTransfer гоняет size байт через loopback при заданных потерях канала.
func runTransfer(t *testing.T, size int, loss float64) {
	t.Helper()

	rcfg := DefaultReceiverConfig()
	rcfg.IdleTimeout = 5 * time.Second
	r := NewReceiver(listen(t, loss), sysclock.New(), rcfg)

	var sink bytes.Buffer
	done := make(chan error, 1)
	go func() {
		_, err := r.ReceiveStream(t.Context(), &sink)
		done <- err
	}()

	scfg := DefaultSenderConfig()
	scfg.RTO = 80 * time.Millisecond
	s := newSender(t, dial(t, r.LocalAddr().String(), "", loss), scfg)

	payload := make([]byte, size)
	rand.New(rand.NewSource(1)).Read(payload)
	if _, err := s.SendStream(t.Context(), bytes.NewReader(payload)); err != nil {
		t.Fatalf("отправка не удалась: %v", err)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("приём не удался: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("приёмник не завершился")
	}

	if sha256.Sum256(sink.Bytes()) != sha256.Sum256(payload) {
		t.Fatalf("данные не совпали: получено %d из %d байт", sink.Len(), size)
	}
}

func TestDeliveryCleanChannel(t *testing.T) { runTransfer(t, 128*1024, 0) }
func TestDeliveryLossy5(t *testing.T)       { runTransfer(t, 128*1024, 0.05) }
func TestDeliveryLossy20(t *testing.T)      { runTransfer(t, 128*1024, 0.2) }
func TestDeliveryEmptyStream(t *testing.T)  { runTransfer(t, 0, 0) }
func TestDeliverySinglePacket(t *testing.T) { runTransfer(t, 10, 0) }
func TestDeliverySmallMTU(t *testing.T)     { runTransfer(t, 8*1024, 0.1) }

// Приёмник должен игнорировать трафик чужой сессии.
func TestReceiverRejectsForeignSession(t *testing.T) {
	r := NewReceiver(listen(t, 0), sysclock.New(), ReceiverConfig{IdleTimeout: 300 * time.Millisecond})

	var sink bytes.Buffer
	done := make(chan struct{})
	go func() { r.ReceiveStream(t.Context(), &sink); close(done) }()

	link := dial(t, r.LocalAddr().String(), "", 0)

	// первый пакет задаёт сессию, второй приходит с чужим session_id
	own, _ := (&domain.Packet{Type: domain.TypeData, Session: 111, Seq: 0, Payload: []byte("ok")}).Encode()
	alien, _ := (&domain.Packet{Type: domain.TypeData, Session: 222, Seq: 1, Payload: []byte("bad")}).Encode()
	link.Send(own, nil)
	link.Send(alien, nil)

	<-done
	if got := sink.String(); got != "ok" {
		t.Fatalf("доставлено %q, ожидалось \"ok\"", got)
	}
	if r.Stats().Rejected != 1 {
		t.Fatalf("отклонено %d пакетов, ожидался 1", r.Stats().Rejected)
	}
}

// Обратный ACK-канал должен приходить на заданный порт, иначе аппаратная
// проверка обратного канала не сможет опираться на постоянный 5-tuple.
func TestSenderFixedLocalPort(t *testing.T) {
	const localPort = 5811

	// напрямую через Server: ReceiveStream ставит свой OnSessionEnd
	c := newCollector()
	cfg := DefaultServerConfig()
	seen := make(chan string, 8)
	cfg.OnSessionEnd = func(info port.SessionInfo, _ ReceiverStats, _ bool) {
		seen <- info.Peer.String()
	}
	srv, stop := startServer(t, cfg, c, 0)
	defer stop()

	scfg := DefaultSenderConfig()
	scfg.RTO = 40 * time.Millisecond
	want := "127.0.0.1:" + strconv.Itoa(localPort)
	s := newSender(t, dial(t, srv.LocalAddr().String(), want, 0), scfg)

	if got := s.LocalAddr().String(); got != want {
		t.Fatalf("сокет привязан к %s, ожидалось %s", got, want)
	}
	if _, err := s.SendStream(t.Context(), bytes.NewReader([]byte("фиксированный порт"))); err != nil {
		t.Fatal(err)
	}

	select {
	case peer := <-seen:
		if peer != want {
			t.Fatalf("приёмник видит источник %s, ожидалось %s", peer, want)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("сессия не завершилась")
	}
}

// Занятый локальный порт должен давать понятную ошибку, а не эфемерный сокет.
func TestSenderLocalPortBusy(t *testing.T) {
	busy, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer busy.Close()

	link, err := udplink.Dial("127.0.0.1:9", busy.LocalAddr().String())
	if err == nil {
		link.Close()
		t.Fatal("ожидалась ошибка занятого порта, сокет открылся")
	}
	if !strings.Contains(err.Error(), "локальный порт") {
		t.Fatalf("непонятная ошибка: %v", err)
	}
}

// Отмена контекста обязана прерывать передачу: без этого Ctrl-C у send убивал
// процесс посреди потока, а общий дедлайн было нечем задать.
func TestSendStreamHonoursContext(t *testing.T) {
	// сокет открыт, но никто не читает и не подтверждает: отправитель уходит
	// в бесконечные повторы, выйти можно только по отмене
	quiet := listen(t, 0)
	link := dial(t, quiet.LocalAddr().String(), "", 0)
	cfg := DefaultSenderConfig()
	cfg.RTO, cfg.MaxRetries = 10*time.Millisecond, 1000
	s := newSender(t, link, cfg)

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		_, err := s.SendStream(ctx, bytes.NewReader(make([]byte, 1<<20)))
		done <- err
	}()

	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("вернулась ошибка %v, ожидался истёкший дедлайн", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("передача не прервалась по истечении дедлайна")
	}
}

// Исчерпание попыток — отдельный отказ: для диода это единственная причина
// бить тревогу, и она обязана быть отличима от ошибок сокета.
func TestSendStreamDeliveryFailedIsSentinel(t *testing.T) {
	link := dial(t, "127.0.0.1:9", "", 1) // канал теряет всё
	cfg := DefaultSenderConfig()
	cfg.RTO, cfg.MaxRetries = time.Millisecond, 2
	s := newSender(t, link, cfg)

	_, err := s.SendStream(t.Context(), bytes.NewReader([]byte("поток")))
	if !errors.Is(err, ErrDeliveryFailed) {
		t.Fatalf("вернулась ошибка %v, ожидалась ErrDeliveryFailed", err)
	}
}
