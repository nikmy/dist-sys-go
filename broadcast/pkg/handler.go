package broadcast

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func NewHandler(node *maelstrom.Node) *handler {
	h := &handler{
		log:         newLog[int64](),
		node:        node,
		undelivered: make(map[string][]int64, 64),
	}

	node.Handle("broadcast", h.broadcast)
	node.Handle("read", h.read)
	node.Handle("topology", h.topology)

	node.Handle("broadcast_ok", h.noAction)
	node.Handle("topology_ok", h.noAction)

	return h
}

type handler struct {
	log  *appendOnlyLog[int64]
	node *maelstrom.Node

	undelivered map[string][]int64
	mu          sync.Mutex
}

type msgBodyBroadcast struct {
	Type    string `json:"type"`
	Message int64  `json:"message,omitempty"`
	Resent  bool   `json:"resent,omitempty"`

	Missed []int64 `json:"missed,omitempty"`
}

type msgBodyRead struct {
	Type     string  `json:"type"`
	Messages []int64 `json:"messages"`
}

type msgBodyTopology struct {
	Type     string              `json:"type"`
	Topology map[string][]string `json:"topology"`
}

func (h *handler) Run() error {
	return h.node.Run()
}

func (h *handler) noAction(maelstrom.Message) error {
	return nil
}

func (h *handler) broadcast(msg maelstrom.Message) error {
	var body msgBodyBroadcast
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	if body.Missed != nil {
		h.log.Append(body.Missed...)
	}

	h.log.Append(body.Message)

	if !body.Resent {
		body.Resent = true
		for _, n := range h.node.NodeIDs() {
			n := n
			body := body
			go func() {
				// If we have undelivered messages for node, try to send them all
				h.mu.Lock()
				missed, haveMissed := h.undelivered[n]
				if haveMissed {
					delete(h.undelivered, n)
					body.Missed = missed
				}
				h.mu.Unlock()

				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()

				if _, err := h.node.SyncRPC(ctx, n, body); err != nil {
					h.mu.Lock()
					h.undelivered[n] = append(h.undelivered[n], body.Missed...)
					h.undelivered[n] = append(h.undelivered[n], body.Message)
					h.mu.Unlock()
				}
			}()
		}
	}
	return h.node.Reply(msg, map[string]string{"type": "broadcast_ok"})
}

func (h *handler) read(msg maelstrom.Message) error {
	var body msgBodyRead
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	body.Messages = h.log.Snapshot()
	body.Type = "read_ok"
	return h.node.Reply(msg, body)
}

func (h *handler) topology(msg maelstrom.Message) error {
	var body msgBodyTopology
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	return h.node.Reply(msg, map[string]string{"type": "topology_ok"})
}
