package broadcast

import (
	"encoding/json"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func NewHandler(node *maelstrom.Node) *handler {
	h := &handler{
		log:  newLog[int64](),
		node: node,
	}

	node.Handle("broadcast", h.broadcast)
	node.Handle("read", h.read)
	node.Handle("topology", h.topology)

	return h
}

type handler struct {
	log  *appendOnlyLog[int64]
	node *maelstrom.Node
	topM sync.Mutex
}

type msgBodyBroadcast struct {
	Type    string `json:"type,omitempty"`
	Message *int64 `json:"message,omitempty"`
}

type msgBodyRead struct {
	Type     string  `json:"type,omitempty"`
	Messages []int64 `json:"messages,omitempty"`
}

type msgBodyTopology struct {
	Type     string              `json:"type,omitempty"`
	Topology map[string][]string `json:"topology,omitempty"`
}

func (h *handler) Run() error {
	return h.node.Run()
}

func (h *handler) broadcast(msg maelstrom.Message) error {
	var body msgBodyBroadcast
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	if body.Message != nil {
		h.log.Append(*body.Message)
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
	id := h.node.ID()
	h.topM.Lock()
	h.node.Init(id, body.Topology[id])
	h.topM.Unlock()
	return h.node.Reply(msg, map[string]string{"type": "topology_ok"})
}
