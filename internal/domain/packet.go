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
	"slices"
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

	crcOffset = 24 // смещение поля crc32 в заголовке
)

// crcZero подставляется вместо самого поля crc32 при подсчёте контрольной
// суммы. Пакет для этого не копируется: сумма считается по трём кускам.
var crcZero [4]byte

// checksum считает CRC32 пакета так, как задано форматом: по заголовку с
// обнулённым полем crc32 плюс payload.
func checksum(raw []byte) uint32 {
	sum := crc32.ChecksumIEEE(raw[:crcOffset])
	sum = crc32.Update(sum, crc32.IEEETable, crcZero[:])
	return crc32.Update(sum, crc32.IEEETable, raw[HeaderLen:])
}

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

// Encode сериализует пакет в новый буфер точного размера.
func (p *Packet) Encode() ([]byte, error) {
	return p.AppendTo(nil)
}

// AppendTo дописывает сериализованный пакет в dst и возвращает результат.
// Позволяет переиспользовать буфер там, где пакеты уходят потоком: ACK на
// каждую сессию иначе аллоцировал бы заголовок на каждое подтверждение.
//
// Вызов вида buf = p.AppendTo(buf[:0]) не аллоцирует, пока ёмкости хватает.
func (p *Packet) AppendTo(dst []byte) ([]byte, error) {
	if len(p.Payload) > 0xFFFF {
		return nil, fmt.Errorf("%w: payload %d байт", ErrBadPacket, len(p.Payload))
	}
	size := HeaderLen + len(p.Payload)
	dst = slices.Grow(dst, size)
	buf := dst[len(dst) : len(dst)+size]
	clear(buf)

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
	binary.BigEndian.PutUint32(buf[crcOffset:], checksum(buf))
	return dst[:len(dst)+size], nil
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

	if checksum(raw) != binary.BigEndian.Uint32(raw[crcOffset:]) {
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
		// буфер канала переиспользуется: без копии payload затрёт следующая
		// датаграмма
		p.Payload = slices.Clone(payload)
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
