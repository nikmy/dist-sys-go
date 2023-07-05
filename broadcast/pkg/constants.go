package broadcast

import "time"

const (
	kClusterSize       = 25
	kInitCapacity      = 64
	kRPCTimeout        = time.Second
	kUpdateInterval    = 800 * time.Millisecond
	kMaxMessagesToSend = 192
)
