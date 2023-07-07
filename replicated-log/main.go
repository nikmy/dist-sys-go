package main

import (
	"context"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	h := NewHandler(
		context.Background(),
		maelstrom.NewNode(),
		NewLog(),
	)

	if err := h.Run(); err != nil {
		log.Fatal(err)
	}
}
