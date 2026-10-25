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

func (n *Node) onCommand(c *conn, cmd rawCommand) outcome {
	id, ok := readID(cmd.ID)
	if !ok {
		return disconnectOutcome(discBadRequest, "bad request")
	}
	method, body := commandMethod(cmd)
	if method == "" {
		if id != 0 {
			return disconnectOutcome(discBadRequest, "bad request")
		}
		n.mu.Lock()
		defer n.mu.Unlock()
		if !c.awaitingPong {
			return disconnectOutcome(discBadRequest, "unexpected pong")
		}
		c.awaitingPong = false
		return outcome{}
	}
	if method != "ping" && !isObject(body) {
		return disconnectOutcome(discBadRequest, "bad request")
	}
	if method != "send" && id == 0 {
		return disconnectOutcome(discBadRequest, "bad request")
	}
	switch method {
	case "connect":
		return n.connectCommand(c, id, body)
	case "subscribe":
		return n.subscribeCommand(c, id, body)
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if !c.protocolConnected {
		return disconnectOutcome(discBadRequest, "bad request")
	}
	switch method {
	case "unsubscribe":
		return n.onUnsubscribe(c, id, body)
	case "publish":
		return n.onPublish(c, id, body)
	case "presence":
		return n.onPresence(c, id, body)
	case "presence_stats":
		return n.onPresenceStats(c, id, body)
	case "history":
		return n.onHistory(c, id, body)
	case "rpc":
		return n.onRPC(id, body)
	case "refresh":
		return outcome{reply: map[string]any{"id": id, "refresh": map[string]any{}}}
	case "send":
		return outcome{}
	case "ping", "sub_refresh":
		return outcome{reply: errorReply(id, errNotAvailable, "not available")}
	default:
		return outcome{reply: errorReply(id, errMethodNotFound, "method not found")}
	}
}
