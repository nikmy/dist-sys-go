package storage

import (
	"context"
	"math/rand"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"

	"kafka/pkg/errors"
	"kafka/pkg/log"
)

func New(node *maelstrom.Node) *logImpl {
	return &logImpl{
		kv:      maelstrom.NewLinKV(node),
		topics:  map[string][]log.Entry{},
		commits: map[string]int{},
	}
}

type logImpl struct {
	kv      *maelstrom.KV
	mu      sync.RWMutex
	topics  map[string][]log.Entry
	commits map[string]int
}

func (l *logImpl) Send(ctx context.Context, key string, msg int) (int, error) {
	/*
		1. Get topic's offset
		2. If no such topic, set write offset to 0
		3. Otherwise, CAS offset to its increment
		4. Append msg to local log
	*/
	offsetKey := kOffsetPrefix + key

	l.mu.Lock()
	defer l.mu.Unlock()

	writeOffset, err := l.kv.ReadInt(ctx, offsetKey)
	if err != nil {
		if !errors.KeyDoesNotExists(err) {
			return 0, err
		}
		writeOffset = 1
	}

	for {
		msgs := l.topics[key]
		if len(msgs) <= writeOffset {
			msgs = make([]log.Entry, writeOffset+4096)
			copy(msgs, l.topics[key])
			l.topics[key] = msgs
		}
		msgs[writeOffset] = log.Entry{Offset: writeOffset, Message: msg}

		err = l.kv.CompareAndSwap(ctx, offsetKey, writeOffset, writeOffset+1, true)

		if err == nil || errors.KeyDoesNotExists(err) {
			break
		}
		if !errors.PreconditionFailed(err) {
			return 0, err
		}
		backoff()

		writeOffset, err = l.kv.ReadInt(ctx, offsetKey)
		if err != nil {
			return 0, err
		}
	}

	return writeOffset, nil
}

func (l *logImpl) Poll(ctx context.Context, key string, offset int) ([]log.Entry, error) {
	/*
			1. Get write offset (0 by default)
			2. If write offset is less than or equal to the given offset, return empty slice
			3. Get messages for each offset greater than or equal to given offset
		       and less than write offset from step 1
	*/
	offsetKey := kOffsetPrefix + key
	writeOffset, err := l.kv.ReadInt(ctx, offsetKey)
	if writeOffset <= offset {
		if err != nil && err.(*maelstrom.RPCError).Code == maelstrom.KeyDoesNotExist {
			err = nil
		}
		return []log.Entry{}, err
	}

	l.mu.RLock()
	defer l.mu.RUnlock()

	msgs := l.topics[key]

	if len(msgs) == 0 {
		return []log.Entry{}, nil
	}

	capacityHint := (writeOffset - offset) / kPartitionFactor
	entries := make([]log.Entry, 0, capacityHint)

	from := 0
	for from < len(msgs) && msgs[from].Offset < offset {
		from++
	}
	for i := from; i < len(msgs) && msgs[i].Offset < writeOffset; i++ {
		if msgs[i].Offset == 0 {
			continue
		}
		entries = append(entries, msgs[i])
	}
	return entries, nil
}

func (l *logImpl) Commit(ctx context.Context, key string, offset int) error {
	/*
		1. Get committed offset
		2. If key not exists, set committed offset equal to the given offset
		3. While committed offset less than given, CAS
	*/
	commitKey := kCommitPrefix + key
	committedOffset, err := l.kv.ReadInt(ctx, commitKey)
	if err != nil && err.(*maelstrom.RPCError).Code != maelstrom.KeyDoesNotExist {
		return err
	}

	for committedOffset < offset {
		err = l.kv.CompareAndSwap(ctx, commitKey, committedOffset, offset, true)
		if err == nil || err.(*maelstrom.RPCError).Code == maelstrom.KeyDoesNotExist {
			break
		}
		if err.(*maelstrom.RPCError).Code != maelstrom.PreconditionFailed {
			return err
		}
		backoff()

		committedOffset, err = l.kv.ReadInt(ctx, commitKey)
		if err != nil {
			return err
		}
	}

	if committedOffset > offset {
		offset = committedOffset
	}

	l.mu.Lock()
	if l.commits[key] < offset {
		l.commits[key] = offset
	}
	l.mu.Unlock()

	return nil
}

func (l *logImpl) GetCommittedOffset(ctx context.Context, key string) (int, error) {
	committedOffset, err := l.kv.ReadInt(ctx, kCommitPrefix+key)
	if err != nil && err.(*maelstrom.RPCError).Code != maelstrom.KeyDoesNotExist {
		return 0, err
	}

	l.mu.RLock()
	localCommittedOffset := l.commits[key]
	l.mu.RUnlock()

	if committedOffset == localCommittedOffset {
		return committedOffset, nil
	}

	l.mu.Lock()
	if l.commits[key] < committedOffset {
		l.commits[key] = committedOffset
	}
	l.mu.Unlock()

	return committedOffset, nil
}

func backoff() {
	time.Sleep(time.Duration(rand.Float32() * float32(kMaxBackoff)))
}
