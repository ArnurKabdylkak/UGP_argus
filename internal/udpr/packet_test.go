package udpr

import (
	"errors"
	"testing"
)

func TestPacketRoundtrip(t *testing.T) {
	p := &Packet{Type: TypeData, Session: 0xDEADBEEF, Seq: 42, Window: 16, Payload: []byte("hello")}
	wire, err := p.Encode()
	if err != nil {
		t.Fatal(err)
	}
	if len(wire) != HeaderLen+5 {
		t.Fatalf("длина пакета %d, ожидалось %d", len(wire), HeaderLen+5)
	}
	got, err := Decode(wire)
	if err != nil {
		t.Fatal(err)
	}
	if got.Type != TypeData || got.Session != 0xDEADBEEF || got.Seq != 42 || string(got.Payload) != "hello" {
		t.Fatalf("пакет разобран неверно: %+v", got)
	}
}

func TestPacketAckFields(t *testing.T) {
	p := &Packet{Type: TypeAck, Session: 1, AckBase: 100, AckBitmap: 0xA5A5A5A5}
	wire, _ := p.Encode()
	got, err := Decode(wire)
	if err != nil {
		t.Fatal(err)
	}
	if got.AckBase != 100 || got.AckBitmap != 0xA5A5A5A5 || len(got.Payload) != 0 {
		t.Fatalf("поля ACK повреждены: %+v", got)
	}
}

func TestDecodeRejects(t *testing.T) {
	base, _ := (&Packet{Type: TypeData, Session: 1, Seq: 1, Payload: []byte("payload")}).Encode()

	corrupt := append([]byte(nil), base...)
	corrupt[len(corrupt)-1] ^= 0xFF

	unknown := append([]byte(nil), base...)
	unknown[1] = 99

	badVer := append([]byte(nil), base...)
	badVer[0] = 7

	badLen := append([]byte(nil), base...)
	badLen[13] = 0xFF

	cases := map[string][]byte{
		"битый CRC":        corrupt,
		"неизвестный тип":  unknown,
		"чужая версия":     badVer,
		"неверная длина":   badLen,
		"обрезанный пакет": base[:10],
	}
	for name, raw := range cases {
		if _, err := Decode(raw); !errors.Is(err, ErrBadPacket) {
			t.Errorf("%s: ожидалась ErrBadPacket, получено %v", name, err)
		}
	}
}
