package realtime

import (
	"bytes"
	"encoding/json"
	"math"
	"time"

	"channeld/internal/config"
	"channeld/internal/engine"
	"channeld/internal/token"
)

type rawCommand struct {
	ID            json.RawMessage `json:"id"`
	Connect       json.RawMessage `json:"connect"`
	Subscribe     json.RawMessage `json:"subscribe"`
	Unsubscribe   json.RawMessage `json:"unsubscribe"`
	Publish       json.RawMessage `json:"publish"`
	Presence      json.RawMessage `json:"presence"`
	PresenceStats json.RawMessage `json:"presence_stats"`
	History       json.RawMessage `json:"history"`
	Ping          json.RawMessage `json:"ping"`
	Send          json.RawMessage `json:"send"`
	RPC           json.RawMessage `json:"rpc"`
	Refresh       json.RawMessage `json:"refresh"`
	SubRefresh    json.RawMessage `json:"sub_refresh"`
}

func (n *Node) onFrame(c *conn, raw []byte) bool {
	var replies []any
	for _, line := range bytes.Split(raw, []byte("\n")) {
		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			continue
		}
		var cmd rawCommand
		if err := json.Unmarshal(line, &cmd); err != nil || !json.Valid(line) {
			n.disconnect(c, discBadRequest, "bad request")
			return false
		}
		outcome := n.onCommand(c, cmd)
		for _, effect := range outcome.effects {
			effect.conn.write(true, effect.payload)
		}
		if outcome.reply != nil {
			replies = append(replies, outcome.reply)
		}
		if outcome.disconnect {
			if len(replies) > 0 {
				c.write(true, mustEncode(replies))
			}
			n.disconnect(c, outcome.code, outcome.reason)
			return false
		}
	}
	if len(replies) > 0 {
		c.write(true, mustEncode(replies))
	}
	return true
}
