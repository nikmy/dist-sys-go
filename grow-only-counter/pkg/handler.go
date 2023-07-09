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
	local state
	node  *maelstrom.Node
	ctx   context.Context
	kv    *maelstrom.KV
}

func (h *handler) Run() error {
	return h.node.Run()
}

func (h *handler) add(req maelstrom.Message) error {
	var body addMsgBody
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return err
	}

	h.local.Add(body.Delta)

	return h.node.Reply(req, map[string]string{"type": "add_ok"})
}

func (h *handler) read(req maelstrom.Message) error {
	var body readMsgBody
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return err
	}

	body.Value = h.local.Get()
	body.Type = "read_ok"

	return h.node.Reply(req, body)
}

func (h *handler) init(maelstrom.Message) error {
	go func() {
		ticker := time.NewTicker(kUpdateInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				h.sync()
			case <-h.ctx.Done():
				return
			}
		}
	}()
	return nil
}

func (h *handler) sync() {
	// "freeze" the state
	h.local.Lock()
	defer h.local.Unlock()

	// obtain delta
	delta := h.local.SwapDelta()

	ctx, cancel := context.WithTimeout(h.ctx, kSyncTimeout)
	defer cancel()

	old, err := h.kv.ReadInt(ctx, kCounterKey)
	if err != nil && !KeyDoesNotExists(err) {
		// fail, roll back delta
		h.local.Add(delta)
		return
	}

	for {
		err := h.kv.CompareAndSwap(ctx, kCounterKey, old, old+int(delta), true)
		if err == nil || KeyDoesNotExists(err) {
			break
		}

		if !PreconditionFailed(err) {
			// fail, roll back delta
			h.local.Add(delta)
			return
		}

		time.Sleep(time.Duration(rand.Float32() * kMaxBackoffTime))
	}

	// update cached value
	h.local.SetCache(int32(old) + delta)
}
