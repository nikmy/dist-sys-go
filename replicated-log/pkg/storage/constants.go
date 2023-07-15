package storage

import "time"

const (
	kOffsetPrefix = "_offset."
	kCommitPrefix = "_commit."

	kPartitionFactor = 2

	kMaxBackoff = time.Millisecond * 100
)
