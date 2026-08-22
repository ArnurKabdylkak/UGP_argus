// Package sysclock реализует port.Clock поверх системных часов.
package sysclock

import "time"

// Clock отдаёт настоящее время.
type Clock struct{}

// New создаёт системные часы.
func New() Clock { return Clock{} }

func (Clock) Now() time.Time { return time.Now() }
