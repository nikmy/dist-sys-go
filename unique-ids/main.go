package main

import (
	"encoding/json"
	"log"
	"sync/atomic"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const maxNodes uint64 = 10

type appNode struct {
	maelstrom.Node
	order uint64
	count atomic.Uint64
}

func main() {
	n := appNode{
		Node:  *maelstrom.NewNode(),
		order: 1,
	}

	n.Handle("init", func(_ maelstrom.Message) error {
		for _, id := range n.NodeIDs() {
			if id < n.ID() {
				n.order++
			}
		}
		return nil
	})

	n.Handle("generate", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		body["type"] = "generate_ok"
		body["id"] = (n.count.Add(1)-1)*maxNodes + n.order
		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
