package broadcast

import "sync"

func newLog[T any]() *appendOnlyLog[T] {
	return &appendOnlyLog[T]{
		log: make([]T, 0, kInitCapacity),
	}
}

type appendOnlyLog[T any] struct {
	mu  sync.RWMutex
	log []T
}

func (l *appendOnlyLog[T]) Append(values ...T) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.log = append(l.log, values...)
}

func (l *appendOnlyLog[T]) Snapshot() []T {
	l.mu.RLock()
	defer l.mu.RUnlock()
	snapshot := make([]T, len(l.log))
	copy(snapshot, l.log)
	return snapshot
}
