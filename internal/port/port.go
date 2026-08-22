// Package port описывает границы, через которые ядро протокола общается
// с внешним миром: канал передачи датаграмм, часы и приёмник данных.
//
// Зависимости направлены внутрь: домен и прикладной слой знают только эти
// интерфейсы, а конкретные реализации живут в internal/adapter.
package port

import (
	"errors"
	"fmt"
	"io"
	"time"
)

// Addr — адрес узла в канале. Конкретный вид зависит от реализации Link:
// для UDP это *net.UDPAddr, для будущего L2-канала — MAC.
type Addr interface {
	String() string
}

// Datagram — принятая датаграмма вместе с адресом источника.
type Datagram struct {
	Payload []byte
	Peer    Addr
}

// ErrTimeout возвращается Recv, когда за отведённое время ничего не пришло.
// Это штатный исход опроса, а не сбой.
var ErrTimeout = errors.New("port: истекло время ожидания")

// ErrClosed возвращается после закрытия канала.
var ErrClosed = errors.New("port: канал закрыт")

// Link — канал передачи датаграмм без гарантий: доставка, порядок и
// однократность не обещаны. Именно поверх него домен строит надёжность.
//
// Смена инкапсуляции (UDP, свой IP-протокол, L2 поверх прямого линка)
// сводится к другой реализации этого интерфейса.
type Link interface {
	// Send отправляет полезную нагрузку. Для канала, связанного с одним
	// узлом, to игнорируется и может быть nil.
	Send(payload []byte, to Addr) error

	// Recv ждёт датаграмму не дольше timeout. По истечении срока возвращает
	// ErrTimeout. Возвращённый Payload действителен до следующего вызова.
	Recv(timeout time.Duration) (Datagram, error)

	// LocalAddr — фактический локальный адрес: тот, на который придут ответы.
	LocalAddr() Addr

	Close() error
}

// IsTimeout сообщает, что Recv вернулся по таймауту.
func IsTimeout(err error) bool { return errors.Is(err, ErrTimeout) }

// Clock отдаёт текущее время. Отдельный интерфейс нужен, чтобы логика окна
// и таймаутов проверялась без ожидания настоящих часов.
type Clock interface {
	Now() time.Time
}

// SessionInfo описывает поток, для которого запрашивается приёмник данных.
type SessionInfo struct {
	Session uint32
	Peer    Addr
	Started time.Time
}

// SinkFactory выдаёт приёмник данных под новую сессию. Возвращённый
// io.WriteCloser закрывается при её завершении.
//
// Через эту границу подключается назначение потока: файл, форвардер в SIEM,
// очередь — прикладной слой о них не знает.
type SinkFactory func(SessionInfo) (io.WriteCloser, error)

// String даёт компактное описание сессии для логов.
func (i SessionInfo) String() string {
	peer := "неизвестен"
	if i.Peer != nil {
		peer = i.Peer.String()
	}
	return fmt.Sprintf("%08x от %s", i.Session, peer)
}
