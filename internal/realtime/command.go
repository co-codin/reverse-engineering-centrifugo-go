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
	n.mu.Lock()
	defer n.mu.Unlock()
	if !c.protocolConnected && method != "connect" {
		return disconnectOutcome(discBadRequest, "bad request")
	}
	if method != "send" && id == 0 {
		return disconnectOutcome(discBadRequest, "bad request")
	}
	switch method {
	case "connect":
		return n.onConnect(c, id, body)
	case "subscribe":
		return n.onSubscribe(c, id, body)
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

func (n *Node) onConnect(c *conn, id uint32, body json.RawMessage) outcome {
	if c.protocolConnected {
		return disconnectOutcome(discBadRequest, "bad request")
	}
	var req struct {
		Token   string                     `json:"token"`
		Name    string                     `json:"name"`
		Version string                     `json:"version"`
		Subs    map[string]json.RawMessage `json:"subs"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return badConnect(id)
	}
	var user string
	var connInfo json.RawMessage
	var channels []string
	if req.Token != "" {
		checked, err := token.Verify(req.Token, n.cfg.TokenSecret, time.Now())
		if err != nil {
			return connectError(id, discUnauthorized, errUnauthorized, "unauthorized")
		}
		if checked.BadRequest {
			return badConnect(id)
		}
		if checked.Expired {
			return connectError(id, discExpired, errTokenExpired, "token expired")
		}
		user = checked.Claims.Sub
		connInfo = checked.Claims.Info
		channels = checked.Claims.Channels
	} else if !n.cfg.Anonymous {
		return connectError(id, discUnauthorized, errUnauthorized, "unauthorized")
	}
	if len(channels) > maxSubs {
		return connectError(id, discBadRequest, errLimitExceeded, "limit exceeded")
	}
	for _, channel := range channels {
		if _, code, message := config.ResolveChannel(n.cfg, channel); code != 0 {
			return connectError(id, discBadRequest, code, message)
		}
	}
	hints := map[string]attachHint{}
	for channel, raw := range req.Subs {
		if !isObject(raw) {
			return badConnect(id)
		}
		var hint struct {
			Recover bool   `json:"recover"`
			Epoch   string `json:"epoch"`
			Offset  uint64 `json:"offset"`
			Flag    int64  `json:"flag"`
		}
		if err := json.Unmarshal(raw, &hint); err != nil {
			return badConnect(id)
		}
		hints[channel] = attachHint{recover: hint.Recover, epoch: hint.Epoch, offset: hint.Offset, flag: hint.Flag}
	}
	if c.connectTimer != nil {
		c.connectTimer.Stop()
	}
	c.user = user
	c.connInfo = connInfo
	c.name = truncate(req.Name, 16)
	c.version = truncate(req.Version, 64)
	c.protocolConnected = true
	n.sessions[c.id] = c
	n.armPing(c)

	subs := map[string]any{}
	var effects []delivery
	for _, channel := range channels {
		ns, _, _ := config.ResolveChannel(n.cfg, channel)
		hint := hints[channel]
		result, joins, _, _ := n.attachLocked(c, channel, ns, hint)
		if result != nil {
			subs[channel] = result
			effects = append(effects, joins...)
		}
	}
	ping := 0
	if n.cfg.PingIntervalMs > 0 {
		ping = int(math.Round(float64(n.cfg.PingIntervalMs) / 1000))
		if ping < 1 {
			ping = 1
		}
	}
	result := map[string]any{
		"client":  c.id,
		"version": n.cfg.Version,
		"ping":    ping,
		"pong":    ping > 0,
		"session": c.session,
		"node":    n.uid,
	}
	if len(subs) > 0 {
		result["subs"] = subs
	}
	return outcome{reply: map[string]any{"id": id, "connect": result}, effects: effects}
}

func (n *Node) onSubscribe(c *conn, id uint32, body json.RawMessage) outcome {
	var req struct {
		Channel     string  `json:"channel"`
		Type        *int    `json:"type"`
		Recover     bool    `json:"recover"`
		Recoverable bool    `json:"recoverable"`
		Positioned  bool    `json:"positioned"`
		Epoch       string  `json:"epoch"`
		Offset      *uint64 `json:"offset"`
		Flag        *int64  `json:"flag"`
	}
	if err := json.Unmarshal(body, &req); err != nil || req.Channel == "" {
		return outcome{reply: errorReply(id, errBadRequest, "bad request")}
	}
	if req.Type != nil && *req.Type != 0 {
		return outcome{reply: errorReply(id, errNotAvailable, "not available")}
	}
	ns, code, message := config.ResolveChannel(n.cfg, req.Channel)
	if code != 0 {
		return outcome{reply: errorReply(id, code, message)}
	}
	if !ns.AllowSubscribeForClient {
		return outcome{reply: errorReply(id, errPermissionDenied, "permission denied")}
	}
	hint := attachHint{recover: req.Recover, recoverable: req.Recoverable, positioned: req.Positioned, epoch: req.Epoch}
	if req.Offset != nil {
		hint.offset = *req.Offset
	}
	if req.Flag != nil {
		hint.flag = *req.Flag
	}
	result, effects, code, message := n.attachLocked(c, req.Channel, ns, hint)
	if code != 0 {
		return outcome{reply: errorReply(id, code, message)}
	}
	return outcome{reply: map[string]any{"id": id, "subscribe": result}, effects: effects}
}

func (n *Node) onUnsubscribe(c *conn, id uint32, body json.RawMessage) outcome {
	var req struct {
		Channel string `json:"channel"`
	}
	if err := json.Unmarshal(body, &req); err != nil || req.Channel == "" {
		return outcome{reply: errorReply(id, errBadRequest, "bad request")}
	}
	effects := n.removeSubLocked(c, req.Channel)
	return outcome{reply: map[string]any{"id": id, "unsubscribe": map[string]any{}}, effects: effects}
}

func (n *Node) onPublish(c *conn, id uint32, body json.RawMessage) outcome {
	var probe map[string]json.RawMessage
	if err := json.Unmarshal(body, &probe); err != nil {
		return outcome{reply: errorReply(id, errBadRequest, "bad request")}
	}
	if _, ok := probe["data"]; !ok {
		return outcome{reply: errorReply(id, errBadRequest, "bad request")}
	}
	var req struct {
		Channel string          `json:"channel"`
		Data    json.RawMessage `json:"data"`
		Type    *int            `json:"type"`
	}
	if err := json.Unmarshal(body, &req); err != nil || req.Channel == "" {
		return outcome{reply: errorReply(id, errBadRequest, "bad request")}
	}
	if req.Type != nil && *req.Type != 0 {
		return outcome{reply: errorReply(id, errNotAvailable, "not available")}
	}
	ns, code, message := config.ResolveChannel(n.cfg, req.Channel)
	if code != 0 {
		return outcome{reply: errorReply(id, code, message)}
	}
	_, subscribed := c.subs[req.Channel]
	if !ns.AllowPublishForClient && !(ns.AllowPublishForSubscriber && subscribed) {
		return outcome{reply: errorReply(id, errPermissionDenied, "permission denied")}
	}
	var chanInfo json.RawMessage
	if sub, ok := c.subs[req.Channel]; ok {
		chanInfo = sub.chanInfo
	}
	_, effects := n.publishLocked(req.Channel, ns, req.Data, false, wireInfo(c, chanInfo), nil, "")
	return outcome{reply: map[string]any{"id": id, "publish": map[string]any{}}, effects: effects}
}

func (n *Node) onPresence(c *conn, id uint32, body json.RawMessage) outcome {
	ns, channel, code, message := n.subscriberChannel(c, body)
	if code != 0 {
		return outcome{reply: errorReply(id, code, message)}
	}
	if !ns.Presence {
		return outcome{reply: errorReply(id, errNotAvailable, "not available")}
	}
	return outcome{reply: map[string]any{"id": id, "presence": map[string]any{"presence": n.wirePresenceLocked(channel)}}}
}

func (n *Node) onPresenceStats(c *conn, id uint32, body json.RawMessage) outcome {
	ns, channel, code, message := n.subscriberChannel(c, body)
	if code != 0 {
		return outcome{reply: errorReply(id, code, message)}
	}
	if !ns.Presence {
		return outcome{reply: errorReply(id, errNotAvailable, "not available")}
	}
	users := map[string]struct{}{}
	set := n.byChannel[channel]
	for _, member := range set {
		users[member.user] = struct{}{}
	}
	return outcome{reply: map[string]any{"id": id, "presence_stats": map[string]any{
		"num_clients": len(set),
		"num_users":   len(users),
	}}}
}

func (n *Node) onHistory(c *conn, id uint32, body json.RawMessage) outcome {
	ns, channel, code, message := n.subscriberChannel(c, body)
	if code != 0 {
		return outcome{reply: errorReply(id, code, message)}
	}
	if ns.HistorySize <= 0 {
		return outcome{reply: errorReply(id, errNotAvailable, "not available")}
	}
	query, code, message := parseHistoryBody(body)
	if code != 0 {
		return outcome{reply: errorReply(id, code, message)}
	}
	history := n.engine.History(channel, streamOptions(ns), query, time.Now().UnixMilli())
	if !history.OK {
		return outcome{reply: errorReply(id, history.Code, history.Message)}
	}
	return outcome{reply: map[string]any{"id": id, "history": historyResult(history)}}
}

func (n *Node) onRPC(id uint32, body json.RawMessage) outcome {
	var req struct {
		Method string          `json:"method"`
		Data   json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return outcome{reply: errorReply(id, errBadRequest, "bad request")}
	}
	if req.Method != "echo" {
		return outcome{reply: errorReply(id, errMethodNotFound, "method not found")}
	}
	data := req.Data
	if len(data) == 0 {
		data = json.RawMessage("null")
	}
	return outcome{reply: map[string]any{"id": id, "rpc": map[string]any{"data": data}}}
}

func (n *Node) subscriberChannel(c *conn, body json.RawMessage) (config.NamespaceConfig, string, int, string) {
	var req struct {
		Channel string `json:"channel"`
	}
	if err := json.Unmarshal(body, &req); err != nil || req.Channel == "" {
		return config.NamespaceConfig{}, "", errBadRequest, "bad request"
	}
	ns, code, message := config.ResolveChannel(n.cfg, req.Channel)
	if code != 0 {
		return config.NamespaceConfig{}, "", code, message
	}
	if _, ok := c.subs[req.Channel]; !ok {
		return config.NamespaceConfig{}, "", errPermissionDenied, "permission denied"
	}
	return ns, req.Channel, 0, ""
}

func parseHistoryBody(body json.RawMessage) (engine.HistoryQuery, int, string) {
	var req struct {
		Limit   *int `json:"limit"`
		Reverse bool `json:"reverse"`
		Since   *struct {
			Offset uint64 `json:"offset"`
			Epoch  string `json:"epoch"`
		} `json:"since"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return engine.HistoryQuery{}, errBadRequest, "bad request"
	}
	limit := 0
	if req.Limit != nil {
		if *req.Limit < 0 {
			limit = maxHistoryLimit
		} else if *req.Limit > maxHistoryLimit {
			limit = maxHistoryLimit
		} else {
			limit = *req.Limit
		}
	}
	query := engine.HistoryQuery{Limit: limit, Reverse: req.Reverse}
	if req.Since != nil {
		query.Since = &engine.Position{Offset: req.Since.Offset, Epoch: req.Since.Epoch}
	}
	return query, 0, ""
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
