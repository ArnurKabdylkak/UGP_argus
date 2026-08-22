// Команда udpr — консольное приложение дата-диода: заворачивает данные в
// UDP + собственный Reliable Transport и доставляет их в точку назначения.
//
//	udpr send -host 10.0.0.2 -port 5555 -file data.bin
//	udpr recv -bind 0.0.0.0 -port 5555 -out data.bin
//	udpr selftest -loss 0.1
package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/argus/udpr/internal/udpr"
)

func main() {
	log.SetFlags(0)
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "send":
		err = cmdSend(os.Args[2:])
	case "recv":
		err = cmdRecv(os.Args[2:])
	case "serve":
		err = cmdServe(os.Args[2:])
	case "selftest":
		err = cmdSelftest(os.Args[2:])
	case "-h", "--help", "help":
		usage()
		return
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "[udpr] ошибка: %v\n", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `udpr — UDP + Reliable Transport для дата-диода

  udpr send     -host HOST [-port N] [-file PATH|-] [-mtu N] [-window N] [-rto D]
                [-lport N | -laddr ADDR] [-loss F] [-v]
  udpr recv     [-bind ADDR] [-port N] [-out PATH|-] [-window N] [-idle D] [-loss F] [-v]
  udpr serve    [-bind ADDR] [-port N] -dir PATH [-window N] [-idle D] [-max N] [-v]
  udpr selftest [-size N] [-loss F] [-window N] [-mtu N] [-rto D] [-v]

Команда serve — постоянно работающий приёмник шлюза: слушает непрерывно,
принимает сессии одну за другой и параллельно, переживает разрывы и
перезапуск отправителей. Останавливается по SIGINT/SIGTERM.

Флаги -lport/-laddr закрепляют локальный порт отправителя: без них ОС выдаёт
эфемерный порт, и обратный ACK-канал нельзя описать постоянным правилом
на межсетевом экране.

Флаг -loss имитирует потери канала и нужен для проверки retransmit.
`)
}

func cmdSend(args []string) error {
	fs := flag.NewFlagSet("send", flag.ExitOnError)
	host := fs.String("host", "", "адрес получателя (обязательно)")
	port := fs.Int("port", 5555, "порт получателя")
	path := fs.String("file", "-", "файл с данными или '-' для stdin")
	mtu := fs.Int("mtu", udpr.MaxPayload, "payload на пакет, байт")
	window := fs.Int("window", 32, "размер окна в пакетах (<=32)")
	rto := fs.Duration("rto", 250*time.Millisecond, "таймаут повторной отправки")
	lport := fs.Int("lport", 0, "фиксированный локальный порт (0 — эфемерный)")
	laddr := fs.String("laddr", "", "фиксированный локальный адрес, перекрывает -lport")
	loss := fs.Float64("loss", 0, "имитация потерь канала, 0..1")
	verbose := fs.Bool("v", false, "подробный лог")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *host == "" {
		return fmt.Errorf("не задан -host")
	}
	warnIgnoredStdin(fs, *path)

	in, closeIn, err := openIn(*path)
	if err != nil {
		return err
	}
	defer closeIn()

	cfg := udpr.DefaultSenderConfig()
	cfg.MTU, cfg.Window, cfg.RTO, cfg.Loss, cfg.Verbose = *mtu, *window, *rto, *loss, *verbose
	switch {
	case *laddr != "":
		cfg.LocalAddr = *laddr
	case *lport != 0:
		cfg.LocalAddr = net.JoinHostPort("", fmt.Sprint(*lport))
	}
	s, err := udpr.NewSender(net.JoinHostPort(*host, fmt.Sprint(*port)), cfg)
	if err != nil {
		return err
	}
	defer s.Close()

	// адрес, на который придёт обратный ACK-поток
	fmt.Fprintf(os.Stderr, "[send] %s -> %s:%d, сессия %08x\n",
		s.LocalAddr(), *host, *port, s.Session())

	start := time.Now()
	stats, err := s.SendStream(in)
	fmt.Fprintf(os.Stderr, "[send] %s, время %.2f c\n", stats, time.Since(start).Seconds())
	if n, reason := s.Guard().Stats(); n > 0 {
		fmt.Fprintf(os.Stderr, "[send] ACK Guard отклонил %d пакетов (последний: %s)\n", n, reason)
	}
	return err
}

func cmdRecv(args []string) error {
	fs := flag.NewFlagSet("recv", flag.ExitOnError)
	bind := fs.String("bind", "0.0.0.0", "адрес прослушивания")
	port := fs.Int("port", 5555, "порт прослушивания")
	path := fs.String("out", "-", "куда писать данные или '-' для stdout")
	window := fs.Int("window", 32, "окно приёма в пакетах (<=32)")
	idle := fs.Duration("idle", 10*time.Second, "выход после тишины")
	loss := fs.Float64("loss", 0, "имитация потерь обратного канала, 0..1")
	verbose := fs.Bool("v", false, "подробный лог")
	if err := fs.Parse(args); err != nil {
		return err
	}

	out, closeOut, err := openOut(*path)
	if err != nil {
		return err
	}
	defer closeOut()

	cfg := udpr.DefaultReceiverConfig()
	cfg.Window, cfg.IdleTimeout, cfg.Loss, cfg.Verbose = *window, *idle, *loss, *verbose
	r, err := udpr.NewReceiver(net.JoinHostPort(*bind, fmt.Sprint(*port)), cfg)
	if err != nil {
		return err
	}
	defer r.Close()

	fmt.Fprintf(os.Stderr, "[recv] слушаю %s\n", r.LocalAddr())
	start := time.Now()
	stats, err := r.ReceiveStream(out)
	fmt.Fprintf(os.Stderr, "[recv] %s, время %.2f c\n", stats, time.Since(start).Seconds())
	return err
}

func cmdServe(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	bind := fs.String("bind", "0.0.0.0", "адрес прослушивания")
	port := fs.Int("port", 5555, "порт прослушивания")
	dir := fs.String("dir", "", "каталог для принятых потоков (обязательно)")
	window := fs.Int("window", 32, "окно приёма в пакетах (<=32)")
	idle := fs.Duration("idle", 60*time.Second, "выселение замолчавшей сессии")
	max := fs.Int("max", 64, "предел одновременных сессий")
	loss := fs.Float64("loss", 0, "имитация потерь обратного канала, 0..1")
	verbose := fs.Bool("v", false, "подробный лог")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *dir == "" {
		return fmt.Errorf("не задан -dir")
	}
	if err := os.MkdirAll(*dir, 0o750); err != nil {
		return err
	}

	cfg := udpr.DefaultServerConfig()
	cfg.Window, cfg.SessionIdle, cfg.MaxSessions = *window, *idle, *max
	cfg.Loss, cfg.Verbose = *loss, *verbose
	cfg.OnSessionEnd = func(info udpr.SessionInfo, stats udpr.ReceiverStats, completed bool) {
		status := "ЗАВЕРШЕНА"
		if !completed {
			// незамеченная потеря потока хуже отставания: это должно быть видно
			status = "ПРЕРВАНА"
		}
		fmt.Fprintf(os.Stderr, "[serve] сессия %s %s: %s, длительность %.1f c\n",
			info, status, stats, time.Since(info.Started).Seconds())
	}

	srv, err := udpr.NewServer(net.JoinHostPort(*bind, fmt.Sprint(*port)), cfg,
		func(info udpr.SessionInfo) (io.WriteCloser, error) {
			name := fmt.Sprintf("%s-%08x.bin", info.Started.UTC().Format("20060102T150405"), info.Session)
			f, err := os.Create(filepath.Join(*dir, name))
			if err != nil {
				return nil, err
			}
			fmt.Fprintf(os.Stderr, "[serve] сессия %s -> %s\n", info, name)
			return &bufferedFile{Writer: bufio.NewWriter(f), file: f}, nil
		})
	if err != nil {
		return err
	}
	defer srv.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	fmt.Fprintf(os.Stderr, "[serve] слушаю %s, каталог %s (Ctrl-C для остановки)\n",
		srv.LocalAddr(), *dir)
	err = srv.Serve(ctx)
	fmt.Fprintf(os.Stderr, "[serve] остановлен, итоги: %s\n", srv.Totals())
	return err
}

// bufferedFile сбрасывает буфер на диск при закрытии сессии.
type bufferedFile struct {
	*bufio.Writer
	file *os.File
}

func (b *bufferedFile) Close() error {
	if err := b.Writer.Flush(); err != nil {
		b.file.Close()
		return err
	}
	return b.file.Close()
}

func cmdSelftest(args []string) error {
	fs := flag.NewFlagSet("selftest", flag.ExitOnError)
	size := fs.Int("size", 256*1024, "размер тестового потока, байт")
	mtu := fs.Int("mtu", udpr.MaxPayload, "payload на пакет, байт")
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

	rcfg := udpr.DefaultReceiverConfig()
	rcfg.Window, rcfg.Loss, rcfg.Verbose, rcfg.IdleTimeout = *window, *loss, *verbose, 5*time.Second
	r, err := udpr.NewReceiver("127.0.0.1:0", rcfg)
	if err != nil {
		return err
	}
	defer r.Close()

	var sink bytes.Buffer
	done := make(chan udpr.ReceiverStats, 1)
	go func() {
		st, _ := r.ReceiveStream(&sink)
		done <- st
	}()

	scfg := udpr.DefaultSenderConfig()
	scfg.MTU, scfg.Window, scfg.RTO, scfg.Loss, scfg.Verbose = *mtu, *window, *rto, *loss, *verbose
	s, err := udpr.NewSender(r.LocalAddr().String(), scfg)
	if err != nil {
		return err
	}
	defer s.Close()

	start := time.Now()
	txStats, err := s.SendStream(bytes.NewReader(payload))
	if err != nil {
		return err
	}
	var rxStats udpr.ReceiverStats
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

// warnIgnoredStdin предупреждает, когда данные пришли в stdin, но задан -file:
// файл побеждает, и переданный поток молча теряется.
func warnIgnoredStdin(fs *flag.FlagSet, path string) {
	explicit := false
	fs.Visit(func(f *flag.Flag) {
		if f.Name == "file" {
			explicit = true
		}
	})
	if !explicit || path == "-" {
		return
	}
	st, err := os.Stdin.Stat()
	if err != nil || st.Mode()&os.ModeCharDevice != 0 {
		return // stdin — терминал, ничего не передавали
	}
	fmt.Fprintf(os.Stderr,
		"[send] ВНИМАНИЕ: в stdin есть данные, но задан -file %s — отправляется файл, stdin игнорируется\n"+
			"[send] чтобы отправить stdin, уберите -file или укажите -file -\n", path)
}

func openIn(path string) (io.Reader, func(), error) {
	if path == "-" || path == "" {
		return bufio.NewReader(os.Stdin), func() {}, nil
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	return bufio.NewReader(f), func() { f.Close() }, nil
}

func openOut(path string) (io.Writer, func(), error) {
	if path == "-" || path == "" {
		w := bufio.NewWriter(os.Stdout)
		return w, func() { w.Flush() }, nil
	}
	f, err := os.Create(path)
	if err != nil {
		return nil, nil, err
	}
	w := bufio.NewWriter(f)
	return w, func() { w.Flush(); f.Close() }, nil
}
