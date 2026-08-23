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

	"github.com/argus/udpr/internal/adapter/filesink"
	"github.com/argus/udpr/internal/adapter/sysclock"
	"github.com/argus/udpr/internal/port"
	"github.com/argus/udpr/internal/usecase"
)

func cmdServe(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	bind := fs.String("bind", envString("UDPR_BIND", "0.0.0.0"), "адрес прослушивания")
	udpPort := fs.Int("port", envInt("UDPR_PORT", 5555), "порт прослушивания")
	dir := fs.String("dir", envString("UDPR_DIR", ""), "каталог для принятых потоков (обязательно)")
	window := fs.Int("window", envInt("UDPR_WINDOW", 32), "окно приёма в пакетах (<=32)")
	idle := fs.Duration("idle", envDuration("UDPR_IDLE", 60*time.Second), "выселение замолчавшей сессии")
	max := fs.Int("max", envInt("UDPR_MAX", 64), "предел одновременных сессий")
	loss := fs.Float64("loss", 0, "имитация потерь обратного канала, 0..1")
	verbose := fs.Bool("v", false, "подробный лог")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *dir == "" {
		return fmt.Errorf("не задан -dir")
	}
	if err := os.MkdirAll(*dir, 0o755); err != nil {
		return err
	}

	cfg := usecase.DefaultServerConfig()
	cfg.Window, cfg.SessionIdle, cfg.MaxSessions = *window, *idle, *max
	cfg.Logger = newLogger(*verbose)
	if err := cfg.Validate(); err != nil {
		return err
	}
	cfg.OnSessionEnd = func(info port.SessionInfo, stats usecase.ReceiverStats, completed bool) {
		status := "ЗАВЕРШЕНА"
		if !completed {
			// незамеченная потеря потока хуже отставания: это должно быть видно
			status = "ПРЕРВАНА"
		}
		fmt.Fprintf(os.Stderr, "[serve] сессия %s %s: %s, длительность %.1f c\n",
			info, status, stats, time.Since(info.Started).Seconds())
	}

	link, closer, err := listenLink(net.JoinHostPort(*bind, fmt.Sprint(*udpPort)), *loss)
	if err != nil {
		return err
	}
	defer closer.Close()

	sink := filesink.Factory(*dir, func(info port.SessionInfo, name string) {
		fmt.Fprintf(os.Stderr, "[serve] сессия %s -> %s\n", info, name)
	})
	srv := usecase.NewServer(link, sysclock.New(), cfg, sink)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	fmt.Fprintf(os.Stderr, "[serve] слушаю %s, каталог %s%s (Ctrl-C для остановки)\n",
		srv.LocalAddr(), *dir, envNote(fs))
	err = srv.Serve(ctx)
	fmt.Fprintf(os.Stderr, "[serve] остановлен, итоги: %s\n", srv.Totals())
	return err
}
