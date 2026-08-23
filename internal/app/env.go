package app

import (
	"flag"
	"os"
	"strconv"
	"strings"
	"time"
)

// Значения по умолчанию берутся из окружения: демон разворачивают unit-файлом
// systemd, где переменные удобнее, чем правка командной строки. Явный флаг
// всегда сильнее переменной, переменная сильнее встроенного умолчания.
//
//	UDPR_BIND UDPR_PORT UDPR_DIR UDPR_WINDOW UDPR_IDLE UDPR_MAX

func envString(name, def string) string {
	if v, ok := os.LookupEnv(name); ok {
		return v
	}
	return def
}

func envInt(name string, def int) int {
	if v, ok := os.LookupEnv(name); ok {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envDuration(name string, def time.Duration) time.Duration {
	if v, ok := os.LookupEnv(name); ok {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

// envNote перечисляет переменные окружения, задавшие настройки этого запуска.
// Печатается рядом с адресом: источник настройки не должен быть сюрпризом,
// когда демон поднят чужим unit-файлом.
func envNote(fs *flag.FlagSet) string {
	explicit := map[string]bool{}
	fs.Visit(func(f *flag.Flag) { explicit[f.Name] = true })

	var used []string
	fs.VisitAll(func(f *flag.Flag) {
		name, ok := flagEnv[f.Name]
		if !ok || explicit[f.Name] {
			return
		}
		if _, set := os.LookupEnv(name); set {
			used = append(used, name)
		}
	})
	if len(used) == 0 {
		return ""
	}
	return " (из окружения: " + strings.Join(used, ", ") + ")"
}

// flagEnv связывает флаг с переменной окружения.
var flagEnv = map[string]string{
	"bind":   "UDPR_BIND",
	"port":   "UDPR_PORT",
	"dir":    "UDPR_DIR",
	"window": "UDPR_WINDOW",
	"idle":   "UDPR_IDLE",
	"max":    "UDPR_MAX",
}
