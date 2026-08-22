package udpr

import (
	"bytes"
	"context"
	"crypto/sha256"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"
)

// runTransfer гоняет size байт через loopback при заданных потерях канала.
func runTransfer(t *testing.T, size int, loss float64) {
	t.Helper()

	rcfg := DefaultReceiverConfig()
	rcfg.Loss, rcfg.IdleTimeout = loss, 5*time.Second
	r, err := NewReceiver("127.0.0.1:0", rcfg)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	var sink bytes.Buffer
	done := make(chan error, 1)
	go func() {
		_, err := r.ReceiveStream(&sink)
		done <- err
	}()

	scfg := DefaultSenderConfig()
	scfg.Loss, scfg.RTO = loss, 80*time.Millisecond
	s, err := NewSender(r.LocalAddr().String(), scfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

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
	r, err := NewReceiver("127.0.0.1:0", ReceiverConfig{IdleTimeout: 300 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	var sink bytes.Buffer
	done := make(chan struct{})
	go func() { r.ReceiveStream(&sink); close(done) }()

	s, err := NewSender(r.LocalAddr().String(), SenderConfig{Session: 111})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// первый пакет задаёт сессию, второй приходит с чужим session_id
	own, _ := (&Packet{Type: TypeData, Session: 111, Seq: 0, Payload: []byte("ok")}).Encode()
	alien, _ := (&Packet{Type: TypeData, Session: 222, Seq: 1, Payload: []byte("bad")}).Encode()
	s.conn.Write(own)
	s.conn.Write(alien)

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
	cfg.OnSessionEnd = func(info SessionInfo, _ ReceiverStats, _ bool) {
		seen <- info.Peer.String()
	}
	srv, err := NewServer("127.0.0.1:0", cfg, c.factory)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go srv.Serve(ctx)

	scfg := DefaultSenderConfig()
	scfg.RTO, scfg.LocalAddr = 40*time.Millisecond, "127.0.0.1:"+strconv.Itoa(localPort)
	s, err := NewSender(srv.LocalAddr().String(), scfg)
	if err != nil {
		t.Fatalf("не удалось занять локальный порт: %v", err)
	}
	defer s.Close()

	want := "127.0.0.1:" + strconv.Itoa(localPort)
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

	cfg := DefaultSenderConfig()
	cfg.LocalAddr = busy.LocalAddr().String()
	s, err := NewSender("127.0.0.1:9", cfg)
	if err == nil {
		s.Close()
		t.Fatal("ожидалась ошибка занятого порта, сокет открылся")
	}
	if !strings.Contains(err.Error(), "локальный порт") {
		t.Fatalf("непонятная ошибка: %v", err)
	}
}
