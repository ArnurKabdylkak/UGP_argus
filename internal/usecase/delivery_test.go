package usecase

import (
	"bytes"
	"crypto/sha256"
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

// runTransfer гоняет size байт через loopback при заданных потерях канала.
func runTransfer(t *testing.T, size int, loss float64) {
	t.Helper()

	rcfg := DefaultReceiverConfig()
	rcfg.IdleTimeout = 5 * time.Second
	r := NewReceiver(listen(t, loss), sysclock.New(), rcfg)

	var sink bytes.Buffer
	done := make(chan error, 1)
	go func() {
		_, err := r.ReceiveStream(&sink)
		done <- err
	}()

	scfg := DefaultSenderConfig()
	scfg.RTO = 80 * time.Millisecond
	s := NewSender(dial(t, r.LocalAddr().String(), "", loss), sysclock.New(), scfg)

	payload := make([]byte, size)
	rand.New(rand.NewSource(1)).Read(payload)
	if _, err := s.SendStream(bytes.NewReader(payload)); err != nil {
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
	go func() { r.ReceiveStream(&sink); close(done) }()

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
	s := NewSender(dial(t, srv.LocalAddr().String(), want, 0), sysclock.New(), scfg)

	if got := s.LocalAddr().String(); got != want {
		t.Fatalf("сокет привязан к %s, ожидалось %s", got, want)
	}
	if _, err := s.SendStream(bytes.NewReader([]byte("фиксированный порт"))); err != nil {
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
