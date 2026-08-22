package domain

// Verdict — исход приёма DATA-пакета.
type Verdict int

const (
	// Accepted — пакет принят в окно приёма.
	Accepted Verdict = iota
	// Duplicate — пакет уже доставлен или уже лежит в буфере.
	Duplicate
	// OutOfWindow — seq за пределами окна приёма; подтверждать нельзя,
	// отправитель повторит позже.
	OutOfWindow
)

// Reassembler собирает непрерывный поток из пакетов, пришедших вразнобой.
// Чистое состояние: ни сети, ни времени, ни ввода-вывода.
type Reassembler struct {
	expected uint32 // первый ещё не полученный seq
	buf      map[uint32][]byte
}

// NewReassembler создаёт сборщик потока, ожидающий seq=0.
func NewReassembler() *Reassembler {
	return &Reassembler{buf: make(map[uint32][]byte)}
}

// Expected возвращает номер первого недостающего пакета — именно он уходит
// в ACK как ack_base.
func (r *Reassembler) Expected() uint32 { return r.expected }

// HasGap сообщает, есть ли в буфере пакеты, лежащие за дыркой.
func (r *Reassembler) HasGap() bool { return len(r.buf) > 0 }

// Accept кладёт пакет в буфер и сообщает, как он классифицирован.
func (r *Reassembler) Accept(seq uint32, payload []byte) Verdict {
	if _, dup := r.buf[seq]; dup || seq < r.expected {
		return Duplicate
	}
	if seq >= r.expected+1+BitmapBits {
		return OutOfWindow
	}
	r.buf[seq] = payload
	return Accepted
}

// InOrder сообщает, пришёл ли пакет ровно на своё место.
func (r *Reassembler) InOrder(seq uint32) bool { return seq == r.expected }

// Drain отдаёт непрерывный префикс потока и продвигает ожидаемый номер.
// Возвращает nil, если первого недостающего пакета всё ещё нет.
func (r *Reassembler) Drain() [][]byte {
	var out [][]byte
	for {
		data, ok := r.buf[r.expected]
		if !ok {
			return out
		}
		delete(r.buf, r.expected)
		out = append(out, data)
		r.expected++
	}
}

// Bitmap строит карту принятых пакетов: бит i соответствует seq = expected+1+i.
func (r *Reassembler) Bitmap() uint32 {
	var bm uint32
	for i := 0; i < BitmapBits; i++ {
		if _, ok := r.buf[r.expected+1+uint32(i)]; ok {
			bm |= 1 << uint(i)
		}
	}
	return bm
}
