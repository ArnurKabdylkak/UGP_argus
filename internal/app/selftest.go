package app

import (
	"bytes"
	"crypto/sha256"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/argus/udpr/internal/adapter/sysclock"
	"github.com/argus/udpr/internal/domain"
	"github.com/argus/udpr/internal/usecase"
)

// cmdSelftest поднимает приёмник и отправителя в одном процессе и сверяет
// SHA-256 исходного и принятого потока — проверка доставки под потерями
// без второго узла.
func cmdSelftest(args []string) error {
	fs := flag.NewFlagSet("selftest", flag.ExitOnError)
	size := fs.Int("size", 256*1024, "размер тестового потока, байт")
	mtu := fs.Int("mtu", domain.MaxPayload, "payload на пакет, байт")
	window := fs.Int("window", 32, "размер окна в пакетах")
	rto := fs.Duration("rto", 100*time.Millisecond, "таймаут повторной отправки")
	loss := fs.Float64("loss", 0, "имитация потерь канала, 0..1")
	verbose := fs.Bool("v", false, "подробный лог")
	if err := fs.Parse(args); err != nil {
		return err
	}

	payload := make([]byte, *size)
	if _, err := rand.New(rand.NewSource(time.Now().UnixNano())).Read(payload); err != nil {
		return err
	}

	rlink, closer, err := listenLink("127.0.0.1:0", *loss)
	if err != nil {
		return err
	}
	defer closer.Close()

	rcfg := usecase.DefaultReceiverConfig()
	rcfg.Window, rcfg.Verbose, rcfg.IdleTimeout = *window, *verbose, 5*time.Second
	r := usecase.NewReceiver(rlink, sysclock.New(), rcfg)

	var sink bytes.Buffer
	done := make(chan usecase.ReceiverStats, 1)
	go func() {
		st, _ := r.ReceiveStream(&sink)
		done <- st
	}()

	scfg := usecase.DefaultSenderConfig()
	scfg.MTU, scfg.Window, scfg.RTO, scfg.Verbose = *mtu, *window, *rto, *verbose
	s, slink, err := dialSender(r.LocalAddr().String(), "", *loss, scfg)
	if err != nil {
		return err
	}
	defer slink.Close()

	start := time.Now()
	txStats, err := s.SendStream(bytes.NewReader(payload))
	if err != nil {
		return err
	}
	var rxStats usecase.ReceiverStats
	select {
	case rxStats = <-done:
	case <-time.After(20 * time.Second):
		return fmt.Errorf("приёмник не завершился за 20 с")
	}
	elapsed := time.Since(start).Seconds()

	got := sink.Bytes()
	ok := sha256.Sum256(got) == sha256.Sum256(payload)
	fmt.Fprintf(os.Stderr, "[send] %s, время %.2f c\n", txStats, elapsed)
	if n, reason := s.Guard().Stats(); n > 0 {
		fmt.Fprintf(os.Stderr, "[send] ACK Guard отклонил %d пакетов (последний: %s)\n", n, reason)
	}
	fmt.Fprintf(os.Stderr, "[recv] %s, время %.2f c\n", rxStats, elapsed)
	status := "OK"
	if !ok {
		status = "ОШИБКА"
	}
	fmt.Printf("[selftest] %d/%d байт, потери канала %.0f%%, целостность: %s\n",
		len(got), len(payload), *loss*100, status)
	if !ok {
		return fmt.Errorf("целостность потока нарушена")
	}
	return nil
}
