package broadcast

import (
	"encoding/json"

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

	node.Handle("broadcast_ok", h.noAction)
	node.Handle("topology_ok", h.noAction)

	return h
}

type handler struct {
	log  *appendOnlyLog[int64]
	node *maelstrom.Node
}

type msgBodyBroadcast struct {
	Type    string `json:"type"`
	Message *int64 `json:"message,omitempty"`
	Resent  bool   `json:"resent,omitempty"`
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
	if body.Message != nil {
		h.log.Append(*body.Message)
		if !body.Resent {
			body.Resent = true
			for _, n := range h.node.NodeIDs() {
				n := n
				go func() {
					_ = h.node.Send(n, body)
				}()
			}
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
