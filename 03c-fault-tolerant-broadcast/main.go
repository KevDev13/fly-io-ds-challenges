package main

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	seenValues := make(map[int]bool)
	var myNeighbors []string
	var seenValuesMut sync.RWMutex

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body struct {
			Message   int `json:"message"`
			MessageId int `json:"msg_id"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		newVal := body.Message
		result := map[string]any{
			"type": "broadcast_ok",
		}

		// check to see if this is a duplicate message
		seenValuesMut.RLock()
		_, ok := seenValues[newVal]
		seenValuesMut.RUnlock()
		if ok {
			return nil
		}

		for _, node := range myNeighbors {
			if node != msg.Src {
				go sendToNeighbor(n, node, newVal)
			}
		}

		seenValuesMut.Lock()
		defer seenValuesMut.Unlock()
		seenValues[newVal] = true

		// if this came from another node, don't send a reply
		if body.MessageId == 0 {
			return nil
		}
		return n.Reply(msg, result)
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		seenValuesMut.RLock()
		defer seenValuesMut.RUnlock()

		var vals []int
		for v := range seenValues {
			vals = append(vals, v)
		}

		result := map[string]any{
			"type":     "read_ok",
			"messages": vals,
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

		myNeighbors = body.Topology[n.ID()]

		result := map[string]any{
			"type": "topology_ok",
		}
		return n.Reply(msg, result)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

func sendToNeighbor(me *maelstrom.Node, node string, msg int) {
	good := false
	newBody := map[string]any{
		"type":    "broadcast",
		"message": msg,
		"msg_id":  0,
	}
	if err := me.RPC(node, newBody, func(msg maelstrom.Message) error {
		good = true
		return nil
	}); err != nil {
		return
	}

	// if after 500 seconds we don't have a response, try, try again
	// duplicates are already taken care of so no issue if receiving node gets this twice
	time.Sleep(time.Millisecond * 500)
	if !good {
		sendToNeighbor(me, node, msg)
	}
}
