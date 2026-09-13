// Package app — точка сборки приложения: разбор командной строки, создание
// адаптеров и связывание их со сценариями из internal/usecase.
//
// Это единственное место, которое знает одновременно и о конкретных адаптерах
// (UDP-канал, системные часы, файловый приёмник), и о сценариях. Всё, что
// делает main, — вызывает Run.
package app

import (
	"fmt"
	"os"
)

// Коды возврата: 0 — успех, 1 — отказ выполнения, 2 — ошибка вызова.
const (
	exitOK    = 0
	exitError = 1
	exitUsage = 2
)

// command — одна подкоманда CLI.
type command func(args []string) error

// Run выполняет команду, заданную аргументами (без имени программы), и
// возвращает код возврата процесса.
func Run(args []string) int {
	if len(args) == 0 {
		usage()
		return exitUsage
	}

	commands := map[string]command{
		"send":     cmdSend,
		"recv":     cmdRecv,
		"serve":    cmdServe,
		"selftest": cmdSelftest,
	}

	switch args[0] {
	case "-h", "--help", "help":
		usage()
		return exitOK
	}

	run, ok := commands[args[0]]
	if !ok {
		usage()
		return exitUsage
	}
	if err := run(args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "[udpr] ошибка: %v\n", err)
		return exitError
	}
	return exitOK
}

func usage() {
	fmt.Fprint(os.Stderr, `udpr — UDP + Reliable Transport для дата-диода

  udpr send     -host HOST [-port N] [-file PATH|-] [-mtu N] [-window N] [-rto D]
                [-min-rto D] [-max-rto D] [-bitrate MBIT]
                [-lport N | -laddr ADDR] [-deadline D] [-loss F] [-v]
  udpr recv     [-bind ADDR] [-port N] [-out PATH|-] [-window N] [-idle D] [-loss F] [-v]
  udpr serve    [-bind ADDR] [-port N] -dir PATH [-window N] [-idle D] [-max N] [-v]
  udpr selftest [-size N] [-loss F] [-window N] [-mtu N] [-rto D] [-v]

Команда serve — постоянно работающий приёмник шлюза: слушает непрерывно,
принимает сессии одну за другой и параллельно, переживает разрывы и
перезапуск отправителей. Останавливается по SIGINT/SIGTERM.

Флаги -lport/-laddr закрепляют локальный порт отправителя: без них ОС выдаёт
эфемерный порт, и обратный ACK-канал нельзя описать постоянным правилом
на межсетевом экране.

Таймаут повтора адаптивный: -rto задаёт лишь начальное значение, дальше он
выводится из измеренного времени оборота и удерживается в -min-rto..-max-rto.

Флаг -bitrate ограничивает скорость выдачи в канал: вспышка на скорости
процессора переполняет очередь сетевой карты и приёмный буфер.

Флаг -loss имитирует потери канала и нужен для проверки retransmit.
Флаг -deadline у send ограничивает передачу по времени целиком.

Значения по умолчанию можно задать окружением: UDPR_BIND, UDPR_PORT, UDPR_DIR,
UDPR_WINDOW, UDPR_IDLE, UDPR_MAX. Явный флаг всегда сильнее переменной.
`)
}
