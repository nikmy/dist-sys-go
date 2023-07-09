package kafka

import (
	"context"
	"encoding/json"
)

type LogEntry struct {
	Offset  int
	Message int64
}

func (l LogEntry) MarshalJSON() ([]byte, error) {
	return json.Marshal([2]int64{int64(l.Offset), l.Message})
}

type LogStorage interface {
	Send(ctx context.Context, key string, msg int64) (int, error)
	Poll(ctx context.Context, key string, offset int) ([]LogEntry, error)
	Commit(ctx context.Context, key string, offset int) error
	GetCommittedOffset(ctx context.Context, key string) (int, error)
}
