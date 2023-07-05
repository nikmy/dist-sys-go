package pkg

import (
	"context"
	"encoding/json"
	"math/rand"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func NewHandler(ctx context.Context, node *maelstrom.Node) *handler {
	h := &handler{
		kv:   maelstrom.NewSeqKV(node),
		ctx:  ctx,
		node: node,
	}
	node.Handle("add", h.add)
	node.Handle("read", h.read)
	node.Handle("init", h.init)
	return h
}

type handler struct {
	node *maelstrom.Node
	ctx  context.Context
	kv   *maelstrom.KV
}

func (h *handler) Run() error {
	return h.node.Run()
}

func (h *handler) add(req maelstrom.Message) error {
	var body addMsgBody
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return err
	}

	for {
		ctx, cancel := context.WithTimeout(h.ctx, kKVTimeout)
		value, err := h.kv.ReadInt(ctx, kCounterKey)
		cancel()
		if err != nil {
			return err
		}

		ctx, cancel = context.WithTimeout(h.ctx, kKVTimeout)
		err = h.kv.CompareAndSwap(ctx, kCounterKey, value, body.Delta+value, false)
		cancel()

		if err == nil {
			break
		}

		if rpcErr, ok := err.(*maelstrom.RPCError); ok && rpcErr.Code == maelstrom.PreconditionFailed {
			time.Sleep(time.Duration(rand.Float32() * kMaxBackoffTime))
		} else {
			return err
		}
	}

	return h.node.Reply(req, map[string]string{"type": "add_ok"})
}

func (h *handler) read(req maelstrom.Message) error {
	var body readMsgBody
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(h.ctx, kKVTimeout)
	defer cancel()
	value, err := h.kv.ReadInt(ctx, kCounterKey)

	if err != nil {
		return err
	}

	body.Value = value
	body.Type = "read_ok"

	return h.node.Reply(req, body)
}

func (h *handler) init(maelstrom.Message) error {
	ctx, cancel := context.WithTimeout(h.ctx, kKVTimeout)
	defer cancel()
	return h.kv.CompareAndSwap(ctx, kCounterKey, 0, 0, true)
}
