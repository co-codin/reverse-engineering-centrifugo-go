package realtime

import (
	"bytes"
	"encoding/json"
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
	case "publish":
		return n.publishCommand(c, id, body)
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if !c.protocolConnected {
		return disconnectOutcome(discBadRequest, "bad request")
	}
	switch method {
	case "unsubscribe":
		return n.onUnsubscribe(c, id, body)
	case "presence":
		return n.onPresence(c, id, body)
	case "presence_stats":
		return n.onPresenceStats(c, id, body)
	case "history":
		return n.onHistory(c, id, body)
	case "rpc":
		return n.onRPC(id, body)
	case "refresh":
		return n.onRefresh(c, id, body)
	case "sub_refresh":
		return n.onSubRefresh(c, id, body)
	case "send":
		return outcome{}
	case "ping":
		return outcome{reply: errorReply(id, errNotAvailable, "not available")}
	default:
		return outcome{reply: errorReply(id, errMethodNotFound, "method not found")}
	}
}

func commandMethod(cmd rawCommand) (string, json.RawMessage) {
	switch {
	case present(cmd.Connect):
		return "connect", cmd.Connect
	case present(cmd.Subscribe):
		return "subscribe", cmd.Subscribe
	case present(cmd.Unsubscribe):
		return "unsubscribe", cmd.Unsubscribe
	case present(cmd.Publish):
		return "publish", cmd.Publish
	case present(cmd.Presence):
		return "presence", cmd.Presence
	case present(cmd.PresenceStats):
		return "presence_stats", cmd.PresenceStats
	case present(cmd.History):
		return "history", cmd.History
	case present(cmd.Ping):
		return "ping", cmd.Ping
	case present(cmd.Send):
		return "send", cmd.Send
	case present(cmd.RPC):
		return "rpc", cmd.RPC
	case present(cmd.Refresh):
		return "refresh", cmd.Refresh
	case present(cmd.SubRefresh):
		return "sub_refresh", cmd.SubRefresh
	default:
		return "", nil
	}
}

func present(raw json.RawMessage) bool {
	return len(raw) > 0 && string(raw) != "null"
}

func isObject(raw json.RawMessage) bool {
	raw = bytes.TrimSpace(raw)
	return len(raw) > 0 && raw[0] == '{'
}

func readID(raw json.RawMessage) (uint32, bool) {
	if !present(raw) {
		return 0, true
	}
	var id uint32
	if err := json.Unmarshal(raw, &id); err != nil {
		return 0, false
	}
	return id, true
}

func errorReply(id uint32, code int, message string) map[string]any {
	return map[string]any{"id": id, "error": map[string]any{"code": code, "message": message}}
}

func disconnectOutcome(code int, reason string) outcome {
	return outcome{disconnect: true, code: code, reason: reason}
}

func badConnect(id uint32) outcome {
	out := connectError(id, discBadRequest, errBadRequest, "bad request")
	return out
}

func connectError(id uint32, closeCode, errorCode int, message string) outcome {
	return outcome{
		reply:      errorReply(id, errorCode, message),
		disconnect: true,
		code:       closeCode,
		reason:     message,
	}
}
