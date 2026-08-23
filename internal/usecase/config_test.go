package usecase

import (
	"testing"
	"time"

	"github.com/argus/udpr/internal/domain"
)

// Ноль означает «взять умолчание» и обязан проходить проверку.
func TestConfigValidateAcceptsZeroAndDefaults(t *testing.T) {
	if err := (SenderConfig{}).Validate(); err != nil {
		t.Errorf("пустой SenderConfig отклонён: %v", err)
	}
	if err := DefaultSenderConfig().Validate(); err != nil {
		t.Errorf("умолчания отправителя отклонены: %v", err)
	}
	if err := (ServerConfig{}).Validate(); err != nil {
		t.Errorf("пустой ServerConfig отклонён: %v", err)
	}
	if err := DefaultServerConfig().Validate(); err != nil {
		t.Errorf("умолчания сервера отклонены: %v", err)
	}
	if err := DefaultReceiverConfig().Validate(); err != nil {
		t.Errorf("умолчания приёмника отклонены: %v", err)
	}
}

// Значения за пределами формата — отказ, а не тихая подмена: оператор
// настраивает канал под конкретное железо и должен узнать о расхождении.
func TestConfigValidateRejectsOutOfRange(t *testing.T) {
	cases := map[string]interface{ Validate() error }{
		"mtu больше payload":   SenderConfig{MTU: domain.MaxPayload + 1},
		"окно больше bitmap":   SenderConfig{Window: domain.BitmapBits + 1},
		"отрицательный rto":    SenderConfig{RTO: -time.Second},
		"отрицательный retry":  SenderConfig{MaxRetries: -1},
		"отрицательный ack":    SenderConfig{AckRate: -1},
		"окно сервера велико":  ServerConfig{Window: domain.BitmapBits + 1},
		"отрицательный idle":   ServerConfig{SessionIdle: -time.Second},
		"отрицательный max":    ServerConfig{MaxSessions: -1},
		"idle приёмника < 0":   ReceiverConfig{IdleTimeout: -time.Second},
		"окно приёмника вел.":  ReceiverConfig{Window: domain.BitmapBits + 1},
		"ack-delay < 0":        ServerConfig{AckDelay: -time.Millisecond},
		"ack-every < 0 сервер": ServerConfig{AckEvery: -1},
	}
	for name, cfg := range cases {
		if err := cfg.Validate(); err == nil {
			t.Errorf("%s: значение принято молча", name)
		}
	}
}
