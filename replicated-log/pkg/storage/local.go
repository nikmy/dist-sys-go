package storage

import (
	"context"
	"sync"

	"kafka/pkg/log"
)

func NewLocal() *localStorage {
	return &localStorage{
		offsets:  map[string]int{},
		messages: map[string][]int{},
	}
}

type localStorage struct {
	m        sync.RWMutex
	o        sync.RWMutex
	offsets  map[string]int
	messages map[string][]int
}

func (l *localStorage) Send(ctx context.Context, key string, msg int) (int, error) {
	l.m.Lock()
	defer l.m.Unlock()
	l.messages[key] = append(l.messages[key], msg)
	return len(l.messages[key]) - 1, nil
}

func (l *localStorage) Poll(_ context.Context, key string, offset int) ([]log.Entry, error) {
	l.m.RLock()
	msgs := l.messages[key]
	l.m.RUnlock()

	if offset >= len(msgs) {
		return []log.Entry{}, nil
	}

	entries := make([]log.Entry, 0, len(msgs[offset:]))
	for j, msg := range msgs[offset:] {
		entries = append(entries, log.Entry{Offset: j + offset, Message: msg})
	}

	return entries, nil
}

func (l *localStorage) Commit(ctx context.Context, key string, offset int) error {
	l.o.Lock()
	defer l.o.Unlock()
	if l.offsets[key] < offset {
		l.offsets[key] = offset
	}
	return nil
}

func (l *localStorage) GetCommittedOffset(_ context.Context, key string) (int, error) {
	l.o.RLock()
	defer l.o.RUnlock()
	return l.offsets[key], nil
}
