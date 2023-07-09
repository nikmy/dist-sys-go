package pkg

import maelstrom "github.com/jepsen-io/maelstrom/demo/go"

func KeyDoesNotExists(err error) bool {
	rpcErr, ok := err.(*maelstrom.RPCError)
	return ok && rpcErr.Code == maelstrom.KeyDoesNotExist
}

func PreconditionFailed(err error) bool {
	rpcErr, ok := err.(*maelstrom.RPCError)
	return ok && rpcErr.Code == maelstrom.PreconditionFailed
}
