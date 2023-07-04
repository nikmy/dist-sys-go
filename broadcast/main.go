package main

import (
	"context"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"

	broadcast "broadcast/pkg"
)

func main() {
	if err := broadcast.NewHandler(context.Background(), maelstrom.NewNode()).Run(); err != nil {
		log.Fatal(err)
	}
}
