package pkg

import "time"

const (
	kMaxBackoffTime = float32(100 * time.Millisecond)
	kKVTimeout      = 1 * time.Second
	kCounterKey     = "g-counter"
)
