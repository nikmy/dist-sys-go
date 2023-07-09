package kafka

import (
	"context"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func NewLogStorage(node *maelstrom.Node) *logImpl {
	return &logImpl{kv: maelstrom.NewLinKV(node)}
}

type logImpl struct {
	kv *maelstrom.KV
}

func (l *logImpl) Send(ctx context.Context, key string, msg int64) (int, error) {
	// TODO implement me
	panic("implement me")
}

func (l *logImpl) Poll(ctx context.Context, key string, offset int) ([]LogEntry, error) {
	// TODO implement me
	panic("implement me")
}

func (l *logImpl) Commit(ctx context.Context, key string, offset int) error {
	// TODO implement me
	panic("implement me")
}

func (l *logImpl) GetCommittedOffset(ctx context.Context, key string) (int, error) {
	// TODO implement me
	panic("implement me")
}
