package domain

import (
	"sort"
	"time"
)

// entry — пакет, отправленный и ещё не подтверждённый.
type entry struct {
	wire   []byte
	sentAt time.Time
	tries  int
	dupGap int
}

// Window — скользящее окно отправителя: какие пакеты в полёте, какие пора
// повторить. Чистое состояние без сети и без часов: время передаётся аргументом.
type Window struct {
	size int
	base uint32 // первый неподтверждённый seq
	next uint32 // следующий свободный seq
	pend map[uint32]*entry
}

// NewWindow создаёт окно заданного размера в пакетах.
func NewWindow(size int) *Window {
	if size <= 0 || size > BitmapBits {
		size = BitmapBits
	}
	return &Window{size: size, pend: make(map[uint32]*entry)}
}

func (w *Window) Base() uint32  { return w.base }
func (w *Window) Next() uint32  { return w.next }
func (w *Window) Empty() bool   { return len(w.pend) == 0 }
func (w *Window) InFlight() int { return len(w.pend) }

// CanQueue сообщает, есть ли место в окне. Занятость считается от base, а не
// от числа буферизованных пакетов: иначе выборочно подтверждённые seq
// позволили бы отправителю уйти за окно приёма.
func (w *Window) CanQueue() bool { return w.next-w.base < uint32(w.size) }

// Queue кладёт готовый к отправке пакет в окно и возвращает его seq.
func (w *Window) Queue(wire []byte) uint32 {
	seq := w.next
	w.pend[seq] = &entry{wire: wire}
	w.next++
	return seq
}

// Wire возвращает байты пакета, если он ещё не подтверждён.
func (w *Window) Wire(seq uint32) ([]byte, bool) {
	e, ok := w.pend[seq]
	if !ok {
		return nil, false
	}
	return e.wire, true
}

// MarkSent отмечает факт отправки и возвращает номер попытки (1 — первая).
func (w *Window) MarkSent(seq uint32, now time.Time) int {
	e, ok := w.pend[seq]
	if !ok {
		return 0
	}
	e.sentAt = now
	e.tries++
	e.dupGap = 0
	return e.tries
}

// Tries возвращает число выполненных попыток отправки пакета.
func (w *Window) Tries(seq uint32) int {
	if e, ok := w.pend[seq]; ok {
		return e.tries
	}
	return 0
}

// Ack применяет подтверждение и возвращает seq, которые следует отправить
// повторно немедленно (fast retransmit).
//
// ack_base — первый ещё НЕ полученный seq; бит i карты соответствует
// seq = ack_base+1+i. Дырка ниже уже подтверждённого пакета означает потерю,
// а не задержку, поэтому её не ждут по таймауту. Повтор разрешается не чаще
// одного раза за rto/2: поток дублирующихся ACK иначе разгоняет лишние
// передачи.
func (w *Window) Ack(ackBase, bitmap uint32, now time.Time, rto time.Duration) []uint32 {
	for seq := range w.pend {
		if seq < ackBase {
			delete(w.pend, seq)
		}
	}
	if ackBase > w.base {
		w.base = ackBase
	}

	var holes []uint32
	var top uint32
	haveTop := false
	for i := 0; i < BitmapBits; i++ {
		seq := ackBase + 1 + uint32(i)
		if bitmap>>uint(i)&1 == 1 {
			delete(w.pend, seq)
			top, haveTop = seq, true
		} else if _, ok := w.pend[seq]; ok {
			holes = append(holes, seq)
		}
	}
	if !haveTop {
		return nil
	}

	var resend []uint32
	for _, seq := range holes {
		if seq >= top {
			continue
		}
		e := w.pend[seq]
		e.dupGap++
		if e.dupGap >= 2 && now.Sub(e.sentAt) >= rto/2 {
			resend = append(resend, seq)
		}
	}
	sort.Slice(resend, func(i, j int) bool { return resend[i] < resend[j] })
	return resend
}

// Expired возвращает seq с истёкшим таймаутом, по возрастанию — порядок
// детерминирован, чтобы поведение не зависело от обхода карты.
func (w *Window) Expired(now time.Time, rto time.Duration) []uint32 {
	var out []uint32
	for seq, e := range w.pend {
		if now.Sub(e.sentAt) >= rto {
			out = append(out, seq)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}
