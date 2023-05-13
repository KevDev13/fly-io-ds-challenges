package main

import (
	"context"
	"encoding/json"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"log"
	"sync"
)

const Key = "count"

func main() {
	me := maelstrom.NewNode()
	kvStore := maelstrom.NewSeqKV(me)
	ctx := context.Background()
	var kvStoreMut sync.RWMutex
	kvStoreMut.Lock() // will get unlocked in "init" handler

	// I finally realized there was an "init" call...
	me.Handle("init", func(msg maelstrom.Message) error {
		defer kvStoreMut.Unlock()

		// see if Key exists; if not, add it
		_, err := kvStore.ReadInt(ctx, Key)
		if err == nil {
			// value exists, so we're good
			return nil
		}
		if err = kvStore.Write(ctx, Key, 0); err != nil {
			return err
		}

		return nil
	})

	me.Handle("add", func(msg maelstrom.Message) error {
		var body struct {
			Delta int `json:"delta"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		kvStoreMut.Lock()
		defer kvStoreMut.Unlock()

		for {
			curVal, err := kvStore.ReadInt(ctx, Key)
			if err != nil {
				// read until no error - don't need to handle the "key not found" since that shouldn't happen
				continue
			}
			if err = kvStore.CompareAndSwap(ctx, Key, curVal, curVal+body.Delta, false); err != nil {
				// compare until no error
				continue
			}
			break
		}

		return me.Reply(msg, map[string]any{
			"type": "add_ok",
		})
	})

	me.Handle("read", func(msg maelstrom.Message) error {
		kvStoreMut.RLock()
		val, err := kvStore.ReadInt(ctx, Key)
		kvStoreMut.RUnlock()
		if err != nil {
			return err
		}

		return me.Reply(msg, map[string]any{
			"type":  "read_ok",
			"value": val,
		})
	})

	if err := me.Run(); err != nil {
		log.Fatal(err)
	}
}
