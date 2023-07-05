package main

import (
	"context"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"

	counter "counter/pkg"
)

func main() {
	if err := counter.NewHandler(context.Background(), maelstrom.NewNode()).Run(); err != nil {
		log.Fatal(err)
	}
}
