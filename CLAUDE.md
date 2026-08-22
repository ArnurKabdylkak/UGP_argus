# UDPR

Консольный дата-диод: UDP как переносчик, гарантии доставки даёт собственный
протокол (seq, окно, ACK base + bitmap, CRC, retransmit).

## Архитектура

Чистая архитектура, зависимости направлены внутрь:

- `internal/domain` — ядро протокола. Никаких сети, файлов и системных часов:
  время приходит параметром. Новых импортов вне stdlib здесь быть не должно.
- `internal/port` — границы наружу: `Link`, `Clock`, `SinkFactory`.
- `internal/usecase` — сценарии (`Sender`, `Server`, `Receiver`) поверх домена и портов.
- `internal/adapter/*` — реализации портов (UDP, часы, файловый приёмник, имитация потерь).
- `internal/app` — точка сборки: разбор командной строки и ручное связывание
  адаптеров со сценариями. Единственное место, где известны конкретные адаптеры.
- `cmd/udpr/main.go` — только `os.Exit(app.Run(os.Args[1:]))`; логику сюда не добавляем.

Внешние зависимости не добавляем: цель — один статический бинарь на stdlib.

## Skills

Always load the `samber/cc-skills-golang@golang-how-to` skill when working on
Go code in this repository.
