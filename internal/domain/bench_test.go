package domain

import (
	"testing"
	"time"
)

var benchWire, _ = (&Packet{Type: TypeData, Session: 1, Seq: 7,
	Payload: make([]byte, MaxPayload)}).Encode()

// Decode лежит на горячем пути приёма: он выполняется для каждой датаграммы.
func BenchmarkDecode(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := Decode(benchWire); err != nil {
			b.Fatal(err)
		}
	}
}

// AppendTo в переиспользуемый буфер — так уходят ACK.
func BenchmarkAppendToReused(b *testing.B) {
	p := &Packet{Type: TypeAck, Session: 1, AckBase: 42, AckBitmap: 0xFF}
	buf := make([]byte, 0, HeaderLen)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var err error
		buf, err = p.AppendTo(buf[:0])
		if err != nil {
			b.Fatal(err)
		}
	}
}

// Полный цикл окна: постановка пакета, отправка, подтверждение.
func BenchmarkWindowQueueAck(b *testing.B) {
	w := NewWindow(BitmapBits)
	wire := make([]byte, HeaderLen)
	now := time.Now()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		seq := w.Queue(wire)
		w.MarkSent(seq, now)
		w.Ack(seq+1, 0, now, time.Second)
	}
}

// Приём пакета по порядку: классификация плюс отдача наверх.
func BenchmarkReassemblerInOrder(b *testing.B) {
	r := NewReassembler()
	payload := make([]byte, MaxPayload)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		r.Accept(uint32(i), payload)
		if err := r.DrainTo(discard{}); err != nil {
			b.Fatal(err)
		}
	}
}

type discard struct{}

func (discard) Write(p []byte) (int, error) { return len(p), nil }
