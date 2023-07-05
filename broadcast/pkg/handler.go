package broadcast

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func NewHandler(ctx context.Context, node *maelstrom.Node) *handler {
	h := &handler{
		ctx:         ctx,
		log:         newLog[int64](),
		node:        node,
		undelivered: make(map[string][]int64, kClusterSize),
	}

	node.Handle("broadcast", h.broadcast)
	node.Handle("read", h.read)
	node.Handle("topology", h.topology)
	node.Handle("init", h.init)

	return h
}

type handler struct {
	ctx  context.Context
	log  *appendOnlyLog[int64]
	node *maelstrom.Node

	undelivered map[string][]int64
	mu          sync.Mutex
}

type msgBodyBroadcast struct {
	Type    string `json:"type"`
	Message int64  `json:"message,omitempty"`

	CatchUp []int64 `json:"catch_up,omitempty"`
}

type msgBodyRead struct {
	Type     string  `json:"type"`
	Messages []int64 `json:"messages"`
}

type msgBodyTopology struct {
	Type     string              `json:"type"`
	Topology map[string][]string `json:"topology"`
}

func (h *handler) init(maelstrom.Message) error {
	for _, follower := range h.node.NodeIDs() {
		follower := follower
		go func() {
			ticker := time.NewTicker(kUpdateInterval)
			for {
				select {
				case <-ticker.C:
					h.updateFollower(follower)
				case <-h.ctx.Done():
					ticker.Stop()
					return
				}
			}
		}()
	}
	return nil
}

func (h *handler) updateFollower(follower string) {
	body := msgBodyBroadcast{
		Type: "broadcast",
	}

	// Pick some undelivered messages
	h.mu.Lock()
	if toSend, ok := h.undelivered[follower]; ok && len(toSend) > 0 {
		if len(toSend) > kMaxMessagesToSend {
			toSend, h.undelivered[follower] = toSend[:kMaxMessagesToSend], toSend[kMaxMessagesToSend:]
		} else {
			delete(h.undelivered, follower)
		}
		body.CatchUp = toSend
	}
	h.mu.Unlock()

	// If there's no messages to send, return
	if len(body.CatchUp) == 0 {
		return
	}

	// Try to send them all to the follower
	ctx, cancel := context.WithTimeout(h.ctx, kRPCTimeout)
	defer cancel()

	if _, err := h.node.SyncRPC(ctx, follower, body); err != nil {
		h.mu.Lock()
		if undelivered, ok := h.undelivered[follower]; ok {
			h.undelivered[follower] = append(body.CatchUp, undelivered...)
		} else {
			h.undelivered[follower] = body.CatchUp
		}
		h.mu.Unlock()
	}
}

func (h *handler) broadcast(req maelstrom.Message) error {
	var body msgBodyBroadcast
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return err
	}
	if body.CatchUp != nil {
		// we have the broadcast message from other node
		h.log.Append(body.CatchUp...)
	} else {
		// we have it from client
		h.log.Append(body.Message)

		for _, n := range h.node.NodeIDs() {
			h.mu.Lock()
			h.undelivered[n] = append(h.undelivered[n], body.Message)
			h.mu.Unlock()
		}
	}
	return h.node.Reply(req, map[string]string{"type": "broadcast_ok"})
}

func (h *handler) read(req maelstrom.Message) error {
	var body msgBodyRead
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return err
	}
	body.Messages = h.log.Snapshot()
	body.Type = "read_ok"
	return h.node.Reply(req, body)
}

func (h *handler) topology(req maelstrom.Message) error {
	var body msgBodyTopology
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return err
	}
	return h.node.Reply(req, map[string]string{"type": "topology_ok"})
}

func (h *handler) Run() error {
	return h.node.Run()
}
