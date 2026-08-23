package domain

import (
	"slices"
	"time"
)

// entry — пакет, отправленный и ещё не подтверждённый.
type entry struct {
	wire   []byte
	sentAt time.Time
	seq    uint32
	tries  int
	dupGap int
	live   bool
}

// Window — скользящее окно отправителя: какие пакеты в полёте, какие пора
// повторить. Чистое состояние без сети и без часов: время передаётся аргументом.
//
// Пакеты лежат в кольце фиксированного размера, а не в карте: формат
// ограничивает окно BitmapBits пакетами, поэтому seq однозначно ложится в
// слот seq % BitmapBits. Это убирает хеширование и аллокации с горячего пути.
// Слот хранит собственный seq: подтверждение может назвать номер за пределами
// окна, и он не должен совпасть с чужим живым слотом.
type Window struct {
	size int
	base uint32 // первый неподтверждённый seq
	next uint32 // следующий свободный seq
	ring [BitmapBits]entry
	live int

	// переиспользуемые буферы результатов: возвращённый срез действителен
	// до следующего вызова того же метода
	resend  []uint32
	expired []uint32
	holes   []uint32
}

// NewWindow создаёт окно заданного размера в пакетах.
func NewWindow(size int) *Window {
	if size <= 0 || size > BitmapBits {
		size = BitmapBits
	}
	return &Window{
		size:    size,
		resend:  make([]uint32, 0, BitmapBits),
		expired: make([]uint32, 0, BitmapBits),
		holes:   make([]uint32, 0, BitmapBits),
	}
}

func (w *Window) Base() uint32  { return w.base }
func (w *Window) Next() uint32  { return w.next }
func (w *Window) Empty() bool   { return w.live == 0 }
func (w *Window) InFlight() int { return w.live }

// slot возвращает запись кольца, если в ней лежит именно этот seq.
func (w *Window) slot(seq uint32) *entry {
	e := &w.ring[seq%BitmapBits]
	if !e.live || e.seq != seq {
		return nil
	}
	return e
}

// drop освобождает слот, если он занят этим seq.
func (w *Window) drop(seq uint32) {
	if e := w.slot(seq); e != nil {
		*e = entry{}
		w.live--
	}
}

// CanQueue сообщает, есть ли место в окне. Занятость считается от base, а не
// от числа буферизованных пакетов: иначе выборочно подтверждённые seq
// позволили бы отправителю уйти за окно приёма.
func (w *Window) CanQueue() bool { return w.next-w.base < uint32(w.size) }

// Queue кладёт готовый к отправке пакет в окно и возвращает его seq.
func (w *Window) Queue(wire []byte) uint32 {
	seq := w.next
	// CanQueue не даёт занять слот живого пакета, но если вызвать Queue в обход
	// проверки, счётчик обязан остаться честным
	if w.ring[seq%BitmapBits].live {
		w.live--
	}
	w.ring[seq%BitmapBits] = entry{wire: wire, seq: seq, live: true}
	w.live++
	w.next++
	return seq
}

// Wire возвращает байты пакета, если он ещё не подтверждён.
func (w *Window) Wire(seq uint32) ([]byte, bool) {
	e := w.slot(seq)
	if e == nil {
		return nil, false
	}
	return e.wire, true
}

// MarkSent отмечает факт отправки и возвращает номер попытки (1 — первая).
func (w *Window) MarkSent(seq uint32, now time.Time) int {
	e := w.slot(seq)
	if e == nil {
		return 0
	}
	e.sentAt = now
	e.tries++
	e.dupGap = 0
	return e.tries
}

// Tries возвращает число выполненных попыток отправки пакета.
func (w *Window) Tries(seq uint32) int {
	if e := w.slot(seq); e != nil {
		return e.tries
	}
	return 0
}

// Ack применяет подтверждение и возвращает seq, которые следует отправить
// повторно немедленно (fast retransmit). Срез действителен до следующего
// вызова Ack.
//
// ack_base — первый ещё НЕ полученный seq; бит i карты соответствует
// seq = ack_base+1+i. Дырка ниже уже подтверждённого пакета означает потерю,
// а не задержку, поэтому её не ждут по таймауту. Повтор разрешается не чаще
// одного раза за rto/2: поток дублирующихся ACK иначе разгоняет лишние
// передачи.
func (w *Window) Ack(ackBase, bitmap uint32, now time.Time, rto time.Duration) []uint32 {
	for i := range w.ring {
		if e := &w.ring[i]; e.live && e.seq < ackBase {
			*e = entry{}
			w.live--
		}
	}
	if ackBase > w.base {
		w.base = ackBase
	}

	w.resend = w.resend[:0]
	w.holes = w.holes[:0]
	var top uint32
	haveTop := false
	for i := 0; i < BitmapBits; i++ {
		seq := ackBase + 1 + uint32(i)
		if bitmap>>uint(i)&1 == 1 {
			w.drop(seq)
			top, haveTop = seq, true
		} else if w.slot(seq) != nil {
			w.holes = append(w.holes, seq)
		}
	}
	if !haveTop {
		return nil
	}

	for _, seq := range w.holes {
		if seq >= top {
			continue
		}
		e := w.slot(seq)
		e.dupGap++
		if e.dupGap >= 2 && now.Sub(e.sentAt) >= rto/2 {
			w.resend = append(w.resend, seq)
		}
	}
	slices.Sort(w.resend)
	return w.resend
}

// Expired возвращает seq с истёкшим таймаутом, по возрастанию — порядок
// детерминирован, чтобы поведение не зависело от обхода кольца. Срез
// действителен до следующего вызова Expired.
func (w *Window) Expired(now time.Time, rto time.Duration) []uint32 {
	w.expired = w.expired[:0]
	for i := range w.ring {
		if e := &w.ring[i]; e.live && now.Sub(e.sentAt) >= rto {
			w.expired = append(w.expired, e.seq)
		}
	}
	slices.Sort(w.expired)
	return w.expired
}
