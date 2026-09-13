package domain

import "time"

// Коэффициенты сглаживания из RFC 6298: alpha = 1/8 для средней оценки,
// beta = 1/4 для разброса. Дробями, чтобы не тащить плавающую точку в
// арифметику таймеров.
const (
	rttAlphaNum, rttAlphaDen = 1, 8
	rttBetaNum, rttBetaDen   = 1, 4
	rttVarFactor             = 4 // RTO = SRTT + 4 * RTTVAR
)

// RTTEstimator оценивает время оборота и выводит из него таймаут повторной
// отправки по RFC 6298.
//
// Фиксированный таймаут — главная причина обвала скорости на реальном канале:
// поток отдаётся строго по порядку, поэтому одна потеря останавливает передачу
// до истечения таймаута. Четверть секунды на каждую потерю при доле потерь в
// один процент роняет скорость в сотни раз. Оценка по факту измеренного
// оборота привязывает ожидание к каналу, а не к настройке.
//
// Состояние чистое: время приходит параметром, часов внутри нет.
type RTTEstimator struct {
	srtt   time.Duration // сглаженная оценка оборота
	rttvar time.Duration // сглаженный разброс
	rto    time.Duration
	min    time.Duration
	max    time.Duration
	have   bool
}

// NewRTTEstimator создаёт оценщик. initial используется, пока нет ни одного
// измерения; min и max ограничивают выведенный таймаут.
func NewRTTEstimator(initial, minRTO, maxRTO time.Duration) *RTTEstimator {
	e := &RTTEstimator{min: minRTO, max: maxRTO}
	if e.min <= 0 {
		e.min = time.Millisecond
	}
	if e.max <= 0 || e.max < e.min {
		e.max = time.Minute
	}
	e.rto = e.clamp(initial)
	return e
}

func (e *RTTEstimator) clamp(d time.Duration) time.Duration {
	switch {
	case d < e.min:
		return e.min
	case d > e.max:
		return e.max
	}
	return d
}

// Sample учитывает измеренный оборот. Вызывать только для пакетов, отправленных
// ровно один раз (алгоритм Карна): по повторно отправленному пакету нельзя
// сказать, какой именно передаче соответствует подтверждение.
func (e *RTTEstimator) Sample(rtt time.Duration) {
	if rtt <= 0 {
		return
	}
	if !e.have {
		e.srtt = rtt
		e.rttvar = rtt / 2
		e.have = true
	} else {
		diff := e.srtt - rtt
		if diff < 0 {
			diff = -diff
		}
		e.rttvar += (diff - e.rttvar) * rttBetaNum / rttBetaDen
		e.srtt += (rtt - e.srtt) * rttAlphaNum / rttAlphaDen
	}
	e.rto = e.clamp(e.srtt + rttVarFactor*e.rttvar)
}

// BackOff удваивает таймаут после истечения. Это защита от неверной оценки:
// если повторы не помогают, ожидание должно расти, а не долбить канал.
func (e *RTTEstimator) BackOff() { e.rto = e.clamp(e.rto * 2) }

// RTO — текущий таймаут повторной отправки.
func (e *RTTEstimator) RTO() time.Duration { return e.rto }

// SRTT — текущая оценка оборота; ноль, пока не было ни одного измерения.
func (e *RTTEstimator) SRTT() time.Duration { return e.srtt }

// ResendGap — минимальный интервал между повторами одного пакета при быстром
// восстановлении. Привязан к обороту, а не к таймауту: карта подтверждений уже
// доказала потерю, ждать полный таймаут незачем, но и чаще одного оборота
// повторять бессмысленно — предыдущая попытка ещё в пути.
func (e *RTTEstimator) ResendGap() time.Duration {
	if !e.have {
		return e.rto / 8
	}
	gap := e.srtt / 2
	if gap < time.Millisecond/4 {
		gap = time.Millisecond / 4
	}
	return gap
}
