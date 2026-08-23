package domain

import "io"

// recvSlots — размер окна приёма: expected плюс BitmapBits номеров, которые
// описывает карта подтверждения.
const recvSlots = BitmapBits + 1

// Verdict — исход приёма DATA-пакета.
type Verdict int

const (
	// VerdictUnknown — нулевое значение: вердикт не выносился. Самый
	// разрешающий исход не должен получаться из забытой инициализации,
	// поэтому Accepted начинается с единицы.
	VerdictUnknown Verdict = iota
	// Accepted — пакет принят в окно приёма.
	Accepted
	// Duplicate — пакет уже доставлен или уже лежит в буфере.
	Duplicate
	// OutOfWindow — seq за пределами окна приёма; подтверждать нельзя,
	// отправитель повторит позже.
	OutOfWindow
)

// String даёт имя вердикта для логов.
func (v Verdict) String() string {
	switch v {
	case Accepted:
		return "accepted"
	case Duplicate:
		return "duplicate"
	case OutOfWindow:
		return "out_of_window"
	}
	return "unknown"
}

// chunk — принятый и ещё не отданный наверх пакет.
type chunk struct {
	data []byte
	seq  uint32
	live bool
}

// Reassembler собирает непрерывный поток из пакетов, пришедших вразнобой.
// Чистое состояние: ни сети, ни времени, ни ввода-вывода.
//
// Окно приёма ограничено форматом: принимаются seq от expected до
// expected+BitmapBits включительно. Поэтому пакеты лежат в кольце из
// recvSlots записей по индексу seq % recvSlots, без карты и без аллокаций
// на пакет. Слот хранит свой seq — за пределами окна номера в кольце
// повторяются.
type Reassembler struct {
	expected uint32
	ring     [recvSlots]chunk
	live     int
}

// NewReassembler создаёт сборщик потока, ожидающий seq=0.
func NewReassembler() *Reassembler {
	return &Reassembler{}
}

// Expected возвращает номер первого недостающего пакета — именно он уходит
// в ACK как ack_base.
func (r *Reassembler) Expected() uint32 { return r.expected }

// HasGap сообщает, есть ли в буфере пакеты, лежащие за дыркой.
func (r *Reassembler) HasGap() bool { return r.live > 0 }

// slot возвращает запись кольца, если в ней лежит именно этот seq.
func (r *Reassembler) slot(seq uint32) *chunk {
	c := &r.ring[seq%recvSlots]
	if !c.live || c.seq != seq {
		return nil
	}
	return c
}

// Accept кладёт пакет в буфер и сообщает, как он классифицирован.
func (r *Reassembler) Accept(seq uint32, payload []byte) Verdict {
	if seq < r.expected || r.slot(seq) != nil {
		return Duplicate
	}
	if seq >= r.expected+recvSlots {
		return OutOfWindow
	}
	r.ring[seq%recvSlots] = chunk{data: payload, seq: seq, live: true}
	r.live++
	return Accepted
}

// InOrder сообщает, пришёл ли пакет ровно на своё место.
func (r *Reassembler) InOrder(seq uint32) bool { return seq == r.expected }

// DrainTo отдаёт непрерывный префикс потока в w и продвигает ожидаемый номер.
// Пока первого недостающего пакета нет, не пишет ничего.
func (r *Reassembler) DrainTo(w io.Writer) error {
	for {
		c := r.slot(r.expected)
		if c == nil {
			return nil
		}
		data := c.data
		*c = chunk{}
		r.live--
		r.expected++
		if _, err := w.Write(data); err != nil {
			return err
		}
	}
}

// Bitmap строит карту принятых пакетов: бит i соответствует seq = expected+1+i.
func (r *Reassembler) Bitmap() uint32 {
	var bm uint32
	for i := 0; i < BitmapBits; i++ {
		if r.slot(r.expected+1+uint32(i)) != nil {
			bm |= 1 << uint(i)
		}
	}
	return bm
}
