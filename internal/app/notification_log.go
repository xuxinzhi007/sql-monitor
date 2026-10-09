package app

import (
	"io"
	"sync"
)

// notificationLog serializes concurrent writes to a single writer.
type notificationLog struct {
	mu     sync.Mutex
	output io.Writer
}

func (l *notificationLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.output.Write(p)
}
