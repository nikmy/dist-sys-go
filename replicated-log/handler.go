package main

import (
	"context"
	"encoding/json"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func NewHandler(ctx context.Context, node *maelstrom.Node, log Log) *handler {
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
	log  Log
}

func (h *handler) Run() error {
	return h.node.Run()
}

func (h *handler) send(req maelstrom.Message) error {
	var body SendRequest
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return err
	}

	offset, err := h.log.Send(h.ctx, body.Key, body.Msg)
	if err != nil {
		return err
	}

	return h.node.Reply(req, NewSendOk(offset))
}

func (h *handler) poll(req maelstrom.Message) error {
	var body PollRequest
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return err
	}

	msgs := make(map[string][]LogEntry, len(body.Offsets))
	for key, offset := range body.Offsets {
		entries, err := h.log.Poll(h.ctx, key, offset)
		if err != nil {
			return err
		}
		msgs[key] = entries
	}

	return h.node.Reply(req, NewPollOk(msgs))
}

func (h *handler) commitOffsets(req maelstrom.Message) error {
	var body CommitOffsetsRequest
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return err
	}

	for key, offset := range body.Offsets {
		err := h.log.Commit(h.ctx, key, offset)
		if err != nil {
			return err
		}
	}

	return h.node.Reply(req, NewCommitOffsetsOk())
}

func (h *handler) listCommittedOffsets(req maelstrom.Message) error {
	var body ListCommittedOffsetsRequest
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return err
	}

	offsets := make(map[string]int, len(body.Keys))
	for _, key := range body.Keys {
		offset, err := h.log.GetCommittedOffset(h.ctx, key)
		if err != nil {
			return err
		}
		offsets[key] = offset
	}

	return h.node.Reply(req, NewListCommittedOffsetsOk(offsets))
}
