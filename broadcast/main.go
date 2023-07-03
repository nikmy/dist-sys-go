package main

import (
	broadcast "broadcast/pkg"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	if err := broadcast.NewHandler(maelstrom.NewNode()).Run(); err != nil {
		log.Fatal(err)
	}
}
