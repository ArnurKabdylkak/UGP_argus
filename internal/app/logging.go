package app

import (
	"log/slog"
	"os"
)

// newLogger собирает логгер диагностики. Обычный режим печатает только
// предупреждения и отказы, -v добавляет отладку. Логи идут в stderr, потому
// что stdout может быть самим потоком данных (udpr recv -out -).
func newLogger(verbose bool) *slog.Logger {
	level := slog.LevelWarn
	if verbose {
		level = slog.LevelDebug
	}
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
}
