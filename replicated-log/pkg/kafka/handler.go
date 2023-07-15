package kafka

import (
	"context"
	"encoding/json"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"

	"kafka/pkg/log"
	"kafka/pkg/message"
)

func New(ctx context.Context, node *maelstrom.Node) *handler {
	h := &handler{
		ctx:  ctx,
		node: node,
		n:    newNode(node),
	}
	node.Handle("init", h.init)
	node.Handle("send", h.send)
	node.Handle("poll", h.poll)
	node.Handle("sync", h.sync)
	node.Handle("commit_offsets", h.commitOffsets)
	node.Handle("list_committed_offsets", h.listCommittedOffsets)
	return h
}

type handler struct {
	ctx  context.Context
	node *maelstrom.Node
	n    Node
}

func (h *handler) Run() error {
	return h.node.Run()
}

func (h *handler) init(maelstrom.Message) error {
	h.n.InitNeighbors()
	go func() {
		ticker := time.NewTicker(kSyncInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				h.sendSync()
			case <-h.ctx.Done():
				return
			}
		}
	}()
	return nil
}

type syncMessage struct {
	Type    string                 `json:"type"`
	Entries map[string][]log.Entry `json:"entries,omitempty"`
}

func (h *handler) sendSync() {
	var body syncMessage

	body.Type = "sync"

	h.n.Freeze()
	defer h.n.Unfreeze()

	ctx, cancel := context.WithTimeout(h.ctx, kSyncTimeout)
	defer cancel()

	for _, n := range h.node.NodeIDs() {
		if n == h.node.ID() {
			continue
		}
		body.Entries = h.n.GetDiff(n)
		if _, err := h.node.SyncRPC(ctx, n, body); err != nil {
			h.n.SetDiff(n, body.Entries)
			return
		}
	}
}

func (h *handler) sync(req maelstrom.Message) error {
	var body syncMessage
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return err
	}

	h.n.SyncEntries(body.Entries)
	return nil
}

func (h *handler) send(req maelstrom.Message) error {
	var body message.SendRequest
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return err
	}

	ctx, cancel := h.contextForStorageRequest()
	defer cancel()
	offset, err := h.n.Send(ctx, body.Key, body.Msg)
	if err != nil {
		return err
	}

	return h.node.Reply(req, message.NewSendOk(offset))
}

func (h *handler) poll(req maelstrom.Message) error {
	var body message.PollRequest
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return err
	}

	msgs := make(map[string][]log.Entry, len(body.Offsets))
	for key, offset := range body.Offsets {
		ctx, cancel := h.contextForStorageRequest()
		entries, err := h.n.Poll(ctx, key, offset)
		cancel()
		if err == nil {
			msgs[key] = entries
		}
	}

	return h.node.Reply(req, message.NewPollOk(msgs))
}

func (h *handler) commitOffsets(req maelstrom.Message) error {
	var body message.CommitOffsetsRequest
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return err
	}

	for key, offset := range body.Offsets {
		ctx, cancel := h.contextForStorageRequest()
		err := h.n.Commit(ctx, key, offset)
		cancel()
		if err != nil {
			return err
		}
	}

	return h.node.Reply(req, message.NewCommitOffsetsOk())
}

func (h *handler) listCommittedOffsets(req maelstrom.Message) error {
	var body message.ListCommittedOffsetsRequest
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return err
	}

	offsets := make(map[string]int, len(body.Keys))
	for _, key := range body.Keys {
		ctx, cancel := h.contextForStorageRequest()
		offset, err := h.n.GetCommit(ctx, key)
		cancel()
		if err != nil {
			return err
		}
		offsets[key] = offset
	}

	return h.node.Reply(req, message.NewListCommittedOffsetsOk(offsets))
}

func (h *handler) contextForStorageRequest() (context.Context, context.CancelFunc) {
	return context.WithTimeout(h.ctx, kStorageTimeout)
}
