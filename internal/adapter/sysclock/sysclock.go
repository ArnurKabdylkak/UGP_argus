// Package sysclock реализует port.Clock поверх системных часов.
package sysclock

import (
	"time"

	"github.com/argus/udpr/internal/port"
)

var _ port.Clock = Clock{}

// Clock отдаёт настоящее время.
type Clock struct{}

// New создаёт системные часы.
func New() Clock { return Clock{} }

func (Clock) Now() time.Time { return time.Now() }
