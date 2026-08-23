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

	"github.com/argus/udpr/internal/adapter/sysclock"
	"github.com/argus/udpr/internal/usecase"
)

func cmdRecv(args []string) error {
	fs := flag.NewFlagSet("recv", flag.ExitOnError)
	bind := fs.String("bind", envString("UDPR_BIND", "0.0.0.0"), "адрес прослушивания")
	port := fs.Int("port", envInt("UDPR_PORT", 5555), "порт прослушивания")
	path := fs.String("out", "-", "куда писать данные или '-' для stdout")
	window := fs.Int("window", envInt("UDPR_WINDOW", 32), "окно приёма в пакетах (<=32)")
	idle := fs.Duration("idle", envDuration("UDPR_IDLE", 10*time.Second), "выход после тишины")
	loss := fs.Float64("loss", 0, "имитация потерь обратного канала, 0..1")
	verbose := fs.Bool("v", false, "подробный лог")
	if err := fs.Parse(args); err != nil {
		return err
	}

	cfg := usecase.DefaultReceiverConfig()
	cfg.Window, cfg.IdleTimeout = *window, *idle
	cfg.Logger = newLogger(*verbose)
	if err := cfg.Validate(); err != nil {
		return err
	}

	out, closeOut, err := openOut(*path)
	if err != nil {
		return err
	}
	defer closeOut()

	link, closer, err := listenLink(net.JoinHostPort(*bind, fmt.Sprint(*port)), *loss)
	if err != nil {
		return err
	}
	defer closer.Close()

	r := usecase.NewReceiver(link, sysclock.New(), cfg)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	fmt.Fprintf(os.Stderr, "[recv] слушаю %s%s\n", r.LocalAddr(), envNote(fs))
	start := time.Now()
	stats, err := r.ReceiveStream(ctx, out)
	fmt.Fprintf(os.Stderr, "[recv] %s, время %.2f c\n", stats, time.Since(start).Seconds())
	return err
}
