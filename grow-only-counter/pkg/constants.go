package pkg

import "time"

const (
	kMaxBackoffTime = float32(100 * time.Millisecond)
	kUpdateInterval = time.Millisecond * 500
	kSyncTimeout    = time.Millisecond * 900
	kCounterKey     = "g-counter"
)
