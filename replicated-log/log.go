package main

import (
	"context"
	"encoding/json"
	"sync"
)

type LogEntry struct {
	Offset  int
	Message int64
}

func (l LogEntry) MarshalJSON() ([]byte, error) {
	return json.Marshal([2]int64{int64(l.Offset), l.Message})
}

type Log interface {
	Send(ctx context.Context, key string, msg int64) (int, error)
	Poll(ctx context.Context, key string, offset int) ([]LogEntry, error)
	Commit(ctx context.Context, key string, offset int) error
	GetCommittedOffset(ctx context.Context, key string) (int, error)
}

func NewLog() *logImpl {
	return &logImpl{
		offsets:  map[string]int{},
		messages: map[string][]int64{},
	}
}

type logImpl struct {
	m        sync.RWMutex
	o        sync.RWMutex
	offsets  map[string]int
	messages map[string][]int64
}

func (l *logImpl) Send(ctx context.Context, key string, msg int64) (int, error) {
	l.m.Lock()
	defer l.m.Unlock()
	l.messages[key] = append(l.messages[key], msg)
	return len(l.messages[key]) - 1, nil
}

func (l *logImpl) Poll(_ context.Context, key string, offset int) ([]LogEntry, error) {
	l.m.RLock()
	msgs := l.messages[key]
	l.m.RUnlock()

	if offset >= len(msgs) {
		return []LogEntry{}, nil
	}

	entries := make([]LogEntry, 0, len(msgs[offset:]))
	for j, msg := range msgs[offset:] {
		entries = append(entries, LogEntry{Offset: j + offset, Message: msg})
	}

	return entries, nil
}

func (l *logImpl) Commit(ctx context.Context, key string, offset int) error {
	l.o.Lock()
	defer l.o.Unlock()
	if l.offsets[key] < offset {
		l.offsets[key] = offset
	}
	return nil
}

func (l *logImpl) GetCommittedOffset(_ context.Context, key string) (int, error) {
	l.o.RLock()
	defer l.o.RUnlock()
	return l.offsets[key], nil
}
