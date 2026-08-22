// Package domain — чистое ядро протокола UDPR: формат пакета, арифметика
// последовательностей, окно отправителя, сборка потока на приёме и политика
// обратного канала.
//
// Слой не знает ни о сети, ни о файлах, ни о системных часах: время приходит
// параметром. Именно этот код подлежит переносу в другую реализацию и именно
// на него пишутся тест-векторы.
package domain

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

// Version — версия формата. Пакеты другой версии отбрасываются.
const Version uint8 = 1

// Типы пакетов. Всё, чего нет в этом списке, приёмник отбрасывает.
const (
	TypeHello uint8 = 1
	TypeData  uint8 = 2
	TypeAck   uint8 = 3
	TypeFin   uint8 = 4
)

// Формат заголовка, big-endian, фиксированные 28 байт:
//
//	 0       1       2       3
//	+-------+-------+-------+-------+
//	| ver   | type  | flags | rsvd  |   4
//	+-------+-------+-------+-------+
//	|          session_id           |   8
//	+-------------------------------+
//	|          sequence             |  12
//	+---------------+---------------+
//	|    length     |    window     |  16
//	+---------------+---------------+
//	|          ack_base             |  20
//	+-------------------------------+
//	|         ack_bitmap            |  24
//	+-------------------------------+
//	|            crc32              |  28
//	+-------------------------------+
//	|          payload ...          |
const (
	HeaderLen  = 28
	BitmapBits = 32 // размер ACK bitmap задан форматом
	MaxPayload = 1400 - HeaderLen
)

// ErrBadPacket — пакет не соответствует формату UDPR.
var ErrBadPacket = errors.New("udpr: некорректный пакет")

// Packet — разобранный пакет UDPR.
type Packet struct {
	Type      uint8
	Flags     uint8
	Session   uint32
	Seq       uint32
	Window    uint16
	AckBase   uint32
	AckBitmap uint32
	Payload   []byte
}

// Encode сериализует пакет и считает CRC32 по заголовку с обнулённым полем CRC
// плюс payload.
func (p *Packet) Encode() ([]byte, error) {
	if len(p.Payload) > 0xFFFF {
		return nil, fmt.Errorf("%w: payload %d байт", ErrBadPacket, len(p.Payload))
	}
	buf := make([]byte, HeaderLen+len(p.Payload))
	buf[0] = Version
	buf[1] = p.Type
	buf[2] = p.Flags
	buf[3] = 0
	binary.BigEndian.PutUint32(buf[4:], p.Session)
	binary.BigEndian.PutUint32(buf[8:], p.Seq)
	binary.BigEndian.PutUint16(buf[12:], uint16(len(p.Payload)))
	binary.BigEndian.PutUint16(buf[14:], p.Window)
	binary.BigEndian.PutUint32(buf[16:], p.AckBase)
	binary.BigEndian.PutUint32(buf[20:], p.AckBitmap)
	copy(buf[HeaderLen:], p.Payload)
	binary.BigEndian.PutUint32(buf[24:], crc32.ChecksumIEEE(buf))
	return buf, nil
}

// Decode разбирает пакет и проверяет версию, тип, длину и CRC.
func Decode(raw []byte) (*Packet, error) {
	if len(raw) < HeaderLen {
		return nil, fmt.Errorf("%w: короче заголовка", ErrBadPacket)
	}
	if raw[0] != Version {
		return nil, fmt.Errorf("%w: версия %d не поддерживается", ErrBadPacket, raw[0])
	}
	typ := raw[1]
	if typ < TypeHello || typ > TypeFin {
		return nil, fmt.Errorf("%w: неизвестный type=%d", ErrBadPacket, typ)
	}
	length := int(binary.BigEndian.Uint16(raw[12:]))
	payload := raw[HeaderLen:]
	if len(payload) != length {
		return nil, fmt.Errorf("%w: length=%d, фактически %d", ErrBadPacket, length, len(payload))
	}

	zeroed := make([]byte, len(raw))
	copy(zeroed, raw)
	binary.BigEndian.PutUint32(zeroed[24:], 0)
	if got := crc32.ChecksumIEEE(zeroed); got != binary.BigEndian.Uint32(raw[24:]) {
		return nil, fmt.Errorf("%w: CRC не сходится", ErrBadPacket)
	}

	p := &Packet{
		Type:      typ,
		Flags:     raw[2],
		Session:   binary.BigEndian.Uint32(raw[4:]),
		Seq:       binary.BigEndian.Uint32(raw[8:]),
		Window:    binary.BigEndian.Uint16(raw[14:]),
		AckBase:   binary.BigEndian.Uint32(raw[16:]),
		AckBitmap: binary.BigEndian.Uint32(raw[20:]),
	}
	if length > 0 {
		p.Payload = append([]byte(nil), payload...)
	}
	return p, nil
}

// TypeName возвращает человекочитаемое имя типа пакета.
func TypeName(t uint8) string {
	switch t {
	case TypeHello:
		return "HELLO"
	case TypeData:
		return "DATA"
	case TypeAck:
		return "ACK"
	case TypeFin:
		return "FIN"
	}
	return "UNKNOWN"
}

func (p *Packet) String() string {
	if p.Type == TypeAck {
		return fmt.Sprintf("<ACK base=%d bitmap=%08x win=%d>", p.AckBase, p.AckBitmap, p.Window)
	}
	return fmt.Sprintf("<%s seq=%d len=%d>", TypeName(p.Type), p.Seq, len(p.Payload))
}
