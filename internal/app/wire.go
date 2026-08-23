package app

import (
	"io"

	"github.com/argus/udpr/internal/adapter/lossylink"
	"github.com/argus/udpr/internal/adapter/sysclock"
	"github.com/argus/udpr/internal/adapter/udplink"
	"github.com/argus/udpr/internal/port"
	"github.com/argus/udpr/internal/usecase"
)

// dialSender открывает канал в сторону remote и собирает отправителя.
// Непустой local закрепляет исходящий порт, loss > 0 добавляет имитацию потерь.
func dialSender(remote, local string, loss float64, cfg usecase.SenderConfig) (*usecase.Sender, io.Closer, error) {
	link, err := udplink.Dial(remote, local)
	if err != nil {
		return nil, nil, err
	}
	sender, err := usecase.NewSender(lossylink.Wrap(link, loss), sysclock.New(), cfg)
	if err != nil {
		link.Close()
		return nil, nil, err
	}
	return sender, link, nil
}

// listenLink открывает канал приёма на bind. Возвращается и обёрнутый канал
// для сценария, и исходный — его закрывает вызывающая сторона.
func listenLink(bind string, loss float64) (port.Link, io.Closer, error) {
	link, err := udplink.Listen(bind)
	if err != nil {
		return nil, nil, err
	}
	return lossylink.Wrap(link, loss), link, nil
}
