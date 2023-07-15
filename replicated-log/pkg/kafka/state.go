package kafka

import (
	"context"
	"kafka/pkg/errors"
	"kafka/pkg/log"
	"math/rand"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Node interface {
	Send(ctx context.Context, topic string, msg int) (int, error)
	Poll(ctx context.Context, topic string, offset int) ([]log.Entry, error)
	Commit(ctx context.Context, topic string, offset int) error
	GetCommit(ctx context.Context, topic string) (int, error)

	InitNeighbors()
	SyncEntries(entries map[string][]log.Entry)

	Freeze()
	Unfreeze()
	GetDiff(string) map[string][]log.Entry
	SetDiff(string, map[string][]log.Entry)
}

func newState() *state {
	return &state{
		log:        map[string][]log.Entry{},
		headOffset: map[string]int{},
		syncOffset: map[string]int{},
		diff:       map[string]map[string][]log.Entry{},
	}
}

type state struct {
	mu  sync.RWMutex
	log map[string][]log.Entry

	syncOffset map[string]int
	headOffset map[string]int

	diff map[string]map[string][]log.Entry
}

func newNode(n *maelstrom.Node) *node {
	return &node{
		s:  newState(),
		n:  n,
		kv: maelstrom.NewLinKV(n),
	}
}

type node struct {
	s  *state
	n  *maelstrom.Node
	kv *maelstrom.KV
}

const (
	kOffsetPath     = "_offset."
	kCommitPath     = "_commit."
	kOffsetLease    = 64
	kMaxBackoff     = float32(time.Millisecond * 100)
	kStorageTimeout = time.Millisecond * 300
	kSyncTimeout    = time.Millisecond * 800
	kSyncInterval   = time.Millisecond * 2000
)

func backoff() {
	time.Sleep(time.Duration(rand.Float32() * kMaxBackoff))
}

func (n *node) InitNeighbors() {
	n.s.mu.Lock()
	defer n.s.mu.Unlock()
	for _, neigh := range n.n.NodeIDs() {
		if neigh == n.n.ID() {
			continue
		}
		n.s.diff[neigh] = make(map[string][]log.Entry)
	}
}

func (n *node) leaseOffset(ctx context.Context, topic string) (int, error) {
	offsetKey := kOffsetPath + topic

	var offset int
	for {
		var err error
		offset, err = n.kv.ReadInt(ctx, offsetKey)
		if err != nil && !errors.KeyDoesNotExists(err) {
			return 0, err
		}

		err = n.kv.CompareAndSwap(ctx, offsetKey, offset, offset+kOffsetLease, true)
		if err == nil || errors.KeyDoesNotExists(err) {
			break
		}
		if !errors.PreconditionFailed(err) {
			return 0, err
		}
		backoff()
	}
	return offset, nil
}

func (n *node) Send(ctx context.Context, topic string, msg int) (int, error) {
	n.s.mu.Lock()
	defer n.s.mu.Unlock()

	offset := n.s.headOffset[topic]
	if offset%kOffsetLease == 0 {
		l, err := n.leaseOffset(ctx, topic)
		if err != nil {
			return 0, err
		}
		offset = l
	}
	n.s.headOffset[topic] = offset + 1

	msgs := n.s.log[topic]

	if currLen := len(msgs); currLen <= offset {
		msgs = append(msgs, make([]log.Entry, offset-currLen+4096)...)
		n.s.log[topic] = msgs
	}
	entry := log.Entry{Offset: offset, Message: msg, Valid: true}
	msgs[offset] = entry
	for neigh := range n.s.diff {
		n.s.diff[neigh][topic] = append(n.s.diff[neigh][topic], entry)
	}

	// n.s.syncOffset[topic]++

	if len(n.s.diff) == 0 && n.s.syncOffset[topic] <= offset {
		n.s.syncOffset[topic] = offset + 1
	}

	return offset, nil
}

func (n *node) Poll(_ context.Context, topic string, offset int) ([]log.Entry, error) {
	n.s.mu.RLock()
	defer n.s.mu.RUnlock()

	syncOffset := n.s.syncOffset[topic]

	if offset >= syncOffset {
		return []log.Entry{}, nil
	}

	msgs := n.s.log[topic]

	entries := make([]log.Entry, syncOffset-offset)
	copy(entries, msgs[offset:syncOffset])
	return entries, nil
}

func (n *node) Commit(ctx context.Context, topic string, offset int) error {
	commitKey := kCommitPath + topic

	cf, err := n.kv.ReadInt(ctx, commitKey)
	if err != nil && !errors.KeyDoesNotExists(err) {
		return err
	}
	if err != nil && errors.KeyDoesNotExists(err) {
		cf = 1
	}

	for cf < offset {
		err = n.kv.CompareAndSwap(ctx, commitKey, cf, offset, true)
		if err == nil || errors.KeyDoesNotExists(err) {
			break
		}
		if !errors.PreconditionFailed(err) {
			return err
		}
		backoff()
	}

	return nil
}

func (n *node) GetCommit(ctx context.Context, topic string) (int, error) {
	commitKey := kCommitPath + topic

	cf, err := n.kv.ReadInt(ctx, commitKey)
	if err == nil {
		return cf, nil
	}
	if errors.KeyDoesNotExists(err) {
		return 1, nil
	}

	return 0, err
}

func (n *node) SyncEntries(entries map[string][]log.Entry) {
	n.s.mu.Lock()
	defer n.s.mu.Unlock()

	for topic, msgs := range entries {
		maxOffset, topicMsgs := msgs[len(msgs)-1].Offset, n.s.log[topic]
		if len(topicMsgs) <= maxOffset {
			topicMsgs = append(topicMsgs, make([]log.Entry, maxOffset-len(topicMsgs)+4096)...)
			n.s.log[topic] = topicMsgs
		}
		for _, entry := range msgs {
			topicMsgs[entry.Offset] = entry
		}

		syncOffset := n.s.syncOffset[topic]
		for syncOffset < len(topicMsgs) && topicMsgs[syncOffset].Valid {
			syncOffset++
		}
		n.s.syncOffset[topic] = syncOffset
	}
}

func (n *node) Freeze() {
	n.s.mu.Lock()
}

func (n *node) Unfreeze() {
	n.s.mu.Unlock()
}

func (n *node) GetDiff(neigh string) map[string][]log.Entry {
	return n.s.diff[neigh]
}

func (n *node) SetDiff(neigh string, diff map[string][]log.Entry) {
	n.s.diff[neigh] = diff
}
