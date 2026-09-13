package domain

import "time"

// Pacer ограничивает скорость выдачи байт в канал — «дырявое ведро».
//
// Без ограничителя отправитель выкидывает всё окно вспышкой на скорости
// процессора. На одной машине это безобидно, на реальном линке вспышка
// переполняет очередь сетевой карты и приёмный буфер сокета; потерянный так
// пакет останавливает поток до повтора. Ровная выдача обходится дешевле, чем
// восстановление после собственных потерь.
//
// Состояние чистое: время приходит параметром.
type Pacer struct {
	rate   float64 // байт в секунду; 0 — без ограничения
	burst  float64 // допустимый всплеск, байт
	tokens float64
	last   time.Time
}

// minPacerBurst — нижний предел всплеска: меньше окна отправителя смысла нет,
// иначе ограничитель дробит и без того мелкие порции.
const minPacerBurst = BitmapBits * MaxPayload

// NewPacer создаёт ограничитель на rate байт в секунду с всплеском burst
// байт. rate <= 0 отключает ограничение.
func NewPacer(rate, burst float64) *Pacer {
	if burst <= 0 {
		// всплеск в одну сотую секунды: достаточно, чтобы не дробить окно,
		// и мало, чтобы не выдать в канал заметный кусок разом
		burst = rate / 100
		if burst < minPacerBurst {
			burst = minPacerBurst
		}
	}
	return &Pacer{rate: rate, burst: burst, tokens: burst}
}

// Enabled сообщает, ограничивает ли пейсер хоть что-нибудь.
func (p *Pacer) Enabled() bool { return p != nil && p.rate > 0 }

// Allow пытается списать n байт. Если бюджета не хватает, возвращает false и
// время, через которое попытка имеет смысл.
func (p *Pacer) Allow(n int, now time.Time) (bool, time.Duration) {
	if !p.Enabled() {
		return true, 0
	}
	if p.last.IsZero() {
		p.last = now
	}
	p.tokens += now.Sub(p.last).Seconds() * p.rate
	if p.tokens > p.burst {
		p.tokens = p.burst
	}
	p.last = now

	need := float64(n)
	if p.tokens >= need {
		p.tokens -= need
		return true, 0
	}
	wait := time.Duration((need - p.tokens) / p.rate * float64(time.Second))
	if wait <= 0 {
		wait = time.Millisecond
	}
	return false, wait
}
