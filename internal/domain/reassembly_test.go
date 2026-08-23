package domain

import (
	"bytes"
	"testing"
)

func TestReassemblerInOrder(t *testing.T) {
	r := NewReassembler()
	var out bytes.Buffer
	for i := 0; i < 3; i++ {
		if v := r.Accept(uint32(i), []byte{byte('a' + i)}); v != Accepted {
			t.Fatalf("seq=%d: вердикт %v", i, v)
		}
		if err := r.DrainTo(&out); err != nil {
			t.Fatal(err)
		}
	}
	if out.String() != "abc" {
		t.Fatalf("доставлено %q, ожидалось \"abc\"", out.String())
	}
	if r.HasGap() || r.Expected() != 3 {
		t.Fatalf("expected=%d, gap=%v", r.Expected(), r.HasGap())
	}
}

// Пакет за дыркой копится в буфере и уходит наверх только после того,
// как недостающий заполнит разрыв.
func TestReassemblerOutOfOrder(t *testing.T) {
	r := NewReassembler()
	var out bytes.Buffer

	r.Accept(1, []byte("b"))
	if err := r.DrainTo(&out); err != nil {
		t.Fatal(err)
	}
	if out.Len() != 0 {
		t.Fatalf("отдано %q до заполнения дырки", out.String())
	}
	if !r.HasGap() || r.Bitmap() != 1<<0 {
		t.Fatalf("bitmap=%08x, ожидался бит seq=1", r.Bitmap())
	}

	r.Accept(0, []byte("a"))
	if err := r.DrainTo(&out); err != nil {
		t.Fatal(err)
	}
	if out.String() != "ab" {
		t.Fatalf("доставлено %q, ожидалось \"ab\"", out.String())
	}
	if r.HasGap() || r.Bitmap() != 0 {
		t.Fatal("буфер не опустел после заполнения дырки")
	}
}

func TestReassemblerVerdicts(t *testing.T) {
	r := NewReassembler()
	r.Accept(0, []byte("a"))
	if v := r.Accept(0, []byte("a")); v != Duplicate {
		t.Fatalf("повтор в буфере: вердикт %v", v)
	}
	if err := r.DrainTo(&bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	if v := r.Accept(0, []byte("a")); v != Duplicate {
		t.Fatalf("повтор уже доставленного: вердикт %v", v)
	}
	// окно приёма — expected..expected+BitmapBits включительно; здесь
	// expected=1, значит последний допустимый seq равен 1+BitmapBits
	if v := r.Accept(1+recvSlots, []byte("x")); v != OutOfWindow {
		t.Fatalf("seq за окном: вердикт %v", v)
	}
	if v := r.Accept(1+BitmapBits, []byte("x")); v != Accepted {
		t.Fatalf("последний seq окна: вердикт %v", v)
	}
}

// Кольцо адресуется по seq % recvSlots: номер ровно на recvSlots вперёд
// попадает в занятый слот и не должен сойти за дубликат.
func TestReassemblerRejectsSlotCollision(t *testing.T) {
	r := NewReassembler()
	r.Accept(1, []byte("b")) // слот 1 занят
	if v := r.Accept(1+recvSlots, []byte("x")); v != OutOfWindow {
		t.Fatalf("seq=%d в занятом слоте: вердикт %v, ожидался OutOfWindow", 1+recvSlots, v)
	}
	if r.Bitmap() != 1<<0 {
		t.Fatalf("bitmap=%08x испорчен чужим seq", r.Bitmap())
	}
}
