package app

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/argus/udpr/internal/domain"
	"github.com/argus/udpr/internal/usecase"
)

func cmdSend(args []string) error {
	fs := flag.NewFlagSet("send", flag.ExitOnError)
	host := fs.String("host", "", "адрес получателя (обязательно)")
	port := fs.Int("port", 5555, "порт получателя")
	path := fs.String("file", "-", "файл с данными или '-' для stdin")
	mtu := fs.Int("mtu", domain.MaxPayload, "payload на пакет, байт")
	window := fs.Int("window", 32, "размер окна в пакетах (<=32)")
	rto := fs.Duration("rto", 250*time.Millisecond, "начальный таймаут повтора (дальше выводится из измеренного RTT)")
	minRTO := fs.Duration("min-rto", 2*time.Millisecond, "нижний предел таймаута повтора")
	maxRTO := fs.Duration("max-rto", 2*time.Second, "верхний предел таймаута повтора")
	bitrate := fs.Float64("bitrate", 0, "предел скорости отправки, Мбит/с (0 — без предела)")
	lport := fs.Int("lport", 0, "фиксированный локальный порт (0 — эфемерный)")
	laddr := fs.String("laddr", "", "фиксированный локальный адрес, перекрывает -lport")
	deadline := fs.Duration("deadline", 0, "общий предел времени передачи (0 — без предела)")
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

	local := *laddr
	if local == "" && *lport != 0 {
		local = net.JoinHostPort("", fmt.Sprint(*lport))
	}
	cfg := usecase.DefaultSenderConfig()
	cfg.MTU, cfg.Window, cfg.RTO = *mtu, *window, *rto
	cfg.MinRTO, cfg.MaxRTO = *minRTO, *maxRTO
	cfg.Bitrate = *bitrate * 1e6
	cfg.Logger = newLogger(*verbose)
	if err := cfg.Validate(); err != nil {
		return err
	}

	s, link, err := dialSender(net.JoinHostPort(*host, fmt.Sprint(*port)), local, *loss, cfg)
	if err != nil {
		return err
	}
	defer link.Close()

	// адрес, на который придёт обратный ACK-поток
	fmt.Fprintf(os.Stderr, "[send] %s -> %s:%d, сессия %08x\n",
		s.LocalAddr(), *host, *port, s.Session())

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if *deadline > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *deadline)
		defer cancel()
	}

	start := time.Now()
	stats, err := s.SendStream(ctx, in)
	fmt.Fprintf(os.Stderr, "[send] %s, время %.2f c, итоговый RTO %s\n",
		stats, time.Since(start).Seconds(), s.RTO().Round(time.Microsecond))
	if n, reason := s.Guard().Stats(); n > 0 {
		fmt.Fprintf(os.Stderr, "[send] ACK Guard отклонил %d пакетов (последний: %s)\n", n, reason)
	}
	return err
}
