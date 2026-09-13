package domain

import (
	"testing"
	"time"
)

func TestPacerDisabledAllowsEverything(t *testing.T) {
	p := NewPacer(0, 0)
	if p.Enabled() {
		t.Fatal("нулевая скорость включила ограничитель")
	}
	for i := 0; i < 1000; i++ {
		if ok, _ := p.Allow(1500, epoch); !ok {
			t.Fatal("ограничитель сработал при выключенном пределе")
		}
	}
}

// Ограничитель обязан держать заданную скорость на длинной дистанции.
func TestPacerHoldsRate(t *testing.T) {
	const rate = 1 << 20 // 1 МБ/с
	p := NewPacer(rate, 4096)

	now := epoch
	sent := 0
	for i := 0; i < 2000; i++ {
		ok, wait := p.Allow(1372, now)
		if ok {
			sent += 1372
			continue
		}
		now = now.Add(wait)
	}
	elapsed := now.Sub(epoch).Seconds()
	if elapsed == 0 {
		t.Fatal("ограничитель не выдал ни одной паузы")
	}
	got := float64(sent) / elapsed
	if got < rate*0.8 || got > rate*1.25 {
		t.Fatalf("держит %.0f байт/с, ожидалось около %d", got, rate)
	}
}

// Всплеск ограничен: нельзя выдать в канал больше, чем разрешено разом.
func TestPacerLimitsBurst(t *testing.T) {
	p := NewPacer(1000, 2000)
	sent := 0
	for {
		ok, _ := p.Allow(500, epoch)
		if !ok {
			break
		}
		sent += 500
		if sent > 10000 {
			t.Fatal("всплеск не ограничен")
		}
	}
	if sent != 2000 {
		t.Fatalf("выдано %d байт разом, ожидалось 2000", sent)
	}
}

func TestPacerRefillsOverTime(t *testing.T) {
	p := NewPacer(1000, 1000)
	if ok, _ := p.Allow(1000, epoch); !ok {
		t.Fatal("первый всплеск не прошёл")
	}
	ok, wait := p.Allow(500, epoch)
	if ok {
		t.Fatal("бюджет исчерпан, а выдача разрешена")
	}
	if wait < 400*time.Millisecond || wait > 600*time.Millisecond {
		t.Fatalf("пауза %s, ожидалось около 500ms", wait)
	}
	if ok, _ := p.Allow(500, epoch.Add(wait)); !ok {
		t.Fatal("после паузы бюджет не восстановился")
	}
}
