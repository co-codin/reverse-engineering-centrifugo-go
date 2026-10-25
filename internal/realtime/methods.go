package realtime

import (
	"encoding/json"
	"time"

	"channeld/internal/config"
	"channeld/internal/engine"
)

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
