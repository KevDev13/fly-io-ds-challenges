package main

import (
	"encoding/json"
	"fmt"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	myId := n.ID()

	var sequenceCount int = 0
	var seenValues []any
	var myNeighbors []string

	n.Handle("echo", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// update msg type to return back
		body["type"] = "echo_ok"

		// echo original msg back with updated type
		return n.Reply(msg, body)
	})

	n.Handle("generate", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// update msg type to return back
		body["type"] = "generate_ok"
		body["id"] = fmt.Sprint(myId, "-", sequenceCount) // node ID + node's sequence count
		sequenceCount += 1

		return n.Reply(msg, body)
	})

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body struct {
			Message int `json:"message"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		seenValues = append(seenValues, body.Message)

		broadcast(n, body.Message, myNeighbors)

		result := map[string]any{
			"type": "broadcast_ok",
		}

		return n.Reply(msg, result)
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		result := map[string]any{
			"type":     "read_ok",
			"messages": seenValues,
		}

		return n.Reply(msg, result)
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		var body struct {
			Topology map[string][]string `json:"topology"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		myNeighbors = body.Topology[myId]

		result := map[string]any{
			"type": "topology_ok",
		}
		return n.Reply(msg, result)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

func broadcast(me *maelstrom.Node, message int, nodes []string) {
	body := map[string]any{
		"type":    "broadcast",
		"message": message,
	}
	for _, node := range nodes {
		err := me.Send(node, body)
		if err != nil {
			return
		}
	}
}
