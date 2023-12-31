package handler

import (
	"context"
	"encoding/json"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"

	"kafka/pkg/log"
	"kafka/pkg/message"
)

func New(ctx context.Context, node *maelstrom.Node, log log.Storage) *handler {
	h := &handler{
		ctx:  ctx,
		node: node,
		log:  log,
	}
	node.Handle("send", h.send)
	node.Handle("poll", h.poll)
	node.Handle("commit_offsets", h.commitOffsets)
	node.Handle("list_committed_offsets", h.listCommittedOffsets)
	return h
}

type handler struct {
	node *maelstrom.Node
	ctx  context.Context
	log  log.Storage
}

func (h *handler) Run() error {
	return h.node.Run()
}

func (h *handler) send(req maelstrom.Message) error {
	var body message.SendRequest
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return err
	}

	ctx, cancel := h.contextForStorageRequest()
	defer cancel()
	offset, err := h.log.Send(ctx, body.Key, body.Msg)
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
		entries, err := h.log.Poll(ctx, key, offset)
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
		err := h.log.Commit(ctx, key, offset)
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
		offset, err := h.log.GetCommittedOffset(ctx, key)
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
