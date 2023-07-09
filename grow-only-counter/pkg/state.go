package pkg

import (
	"sync"
	"sync/atomic"
)

/*
		state represents local counter and provides following semantics:
			1. To increase counter, just invoke Add() method
			2. To get counter's value, invoke Get()
			3. To update counter (for synchronization), execute call sequence
	           Lock -> SwapDelta -> SetCache -> Unlock
*/
type state struct {
	delta atomic.Int32
	cache int32
	mu    sync.RWMutex
}

func (s *state) Add(delta int32) {
	s.delta.Add(delta)
}

func (s *state) Get() int32 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.cache + s.delta.Load()
}

func (s *state) SwapDelta() int32 {
	return s.delta.Swap(0)
}

func (s *state) SetCache(value int32) {
	s.cache = value
}

func (s *state) Lock() {
	s.mu.Lock()
}

func (s *state) Unlock() {
	s.mu.Unlock()
}
