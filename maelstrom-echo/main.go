package main

import (
    "encoding/json"
    "log"

    maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
    n := maelstrom.NewNode()
    
    n.Handle("echo", func(msg maelstrom.Message) error {
        var body map[string]any
        if err := json.Unmarshal(msg.Body, &body); err != nil {
            return err
        }

        // update msg type to return back
        body["type"] = "echo_ok"

        // echo original mesg back with updated type
        return n.Reply(msg, body)
    })

    if err := n.Run(); err != nil {
        log.Fatal(err)
    }
}
