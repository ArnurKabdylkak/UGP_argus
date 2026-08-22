package app

import (
	"flag"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/argus/udpr/internal/adapter/sysclock"
	"github.com/argus/udpr/internal/usecase"
)

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

	link, closer, err := listenLink(net.JoinHostPort(*bind, fmt.Sprint(*port)), *loss)
	if err != nil {
		return err
	}
	defer closer.Close()

	cfg := usecase.DefaultReceiverConfig()
	cfg.Window, cfg.IdleTimeout, cfg.Verbose = *window, *idle, *verbose
	r := usecase.NewReceiver(link, sysclock.New(), cfg)

	fmt.Fprintf(os.Stderr, "[recv] слушаю %s\n", r.LocalAddr())
	start := time.Now()
	stats, err := r.ReceiveStream(out)
	fmt.Fprintf(os.Stderr, "[recv] %s, время %.2f c\n", stats, time.Since(start).Seconds())
	return err
}
