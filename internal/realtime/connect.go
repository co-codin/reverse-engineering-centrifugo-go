package realtime

import (
	"encoding/json"
	"math"
	"time"

	"channeld/internal/config"
	"channeld/internal/token"
)

type connectDecision struct {
	name     string
	version  string
	user     string
	connInfo json.RawMessage
	channels []string
	data     json.RawMessage
	hints    map[string]attachHint
	expireAt int64
}

func (n *Node) connectCommand(c *conn, id uint32, body json.RawMessage) outcome {
	decision, bad := n.prepareConnect(c, id, body)
	if bad != nil {
		return *bad
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if c.dropped.Load() {
		return outcome{}
	}
	if c.protocolConnected {
		return disconnectOutcome(discBadRequest, "bad request")
	}
	return n.finishConnect(c, id, decision)
}

func (n *Node) prepareConnect(c *conn, id uint32, body json.RawMessage) (connectDecision, *outcome) {
	var req struct {
		Token   string                     `json:"token"`
		Name    string                     `json:"name"`
		Version string                     `json:"version"`
		Data    json.RawMessage            `json:"data"`
		Subs    map[string]json.RawMessage `json:"subs"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		bad := badConnect(id)
		return connectDecision{}, &bad
	}
	decision := connectDecision{
		name:    req.Name,
		version: req.Version,
		hints:   map[string]attachHint{},
	}
	for channel, raw := range req.Subs {
		if !isObject(raw) {
			bad := badConnect(id)
			return connectDecision{}, &bad
		}
		var hint struct {
			Recover bool   `json:"recover"`
			Epoch   string `json:"epoch"`
			Offset  uint64 `json:"offset"`
			Flag    int64  `json:"flag"`
		}
		if err := json.Unmarshal(raw, &hint); err != nil {
			bad := badConnect(id)
			return connectDecision{}, &bad
		}
		decision.hints[channel] = attachHint{recover: hint.Recover, epoch: hint.Epoch, offset: hint.Offset, flag: hint.Flag}
	}
	if req.Token != "" {
		checked, err := token.Verify(req.Token, n.cfg.TokenSecret, time.Now())
		if err != nil {
			bad := connectError(id, discUnauthorized, errUnauthorized, "unauthorized")
			return connectDecision{}, &bad
		}
		if checked.BadRequest {
			bad := badConnect(id)
			return connectDecision{}, &bad
		}
		if checked.Expired {
			bad := connectError(id, discExpired, errTokenExpired, "token expired")
			return connectDecision{}, &bad
		}
		decision.user = checked.Claims.Sub
		decision.connInfo = checked.Claims.Info
		decision.channels = checked.Claims.Channels
	} else if n.cfg.ConnectProxyEnabled {
		proxied, bad := n.connectProxy(c, id, req.Name, req.Version, req.Data)
		if bad != nil {
			return connectDecision{}, bad
		}
		decision.user = proxied.User
		decision.connInfo = proxied.Info
		decision.channels = proxied.Channels
		decision.data = proxied.Data
		decision.expireAt = proxied.ExpireAt
	} else if !n.cfg.Anonymous {
		bad := connectError(id, discUnauthorized, errUnauthorized, "unauthorized")
		return connectDecision{}, &bad
	}
	if decision.expireAt > 0 && decision.expireAt <= time.Now().Unix() {
		bad := connectError(id, discExpired, errTokenExpired, "token expired")
		return connectDecision{}, &bad
	}
	if len(decision.channels) > maxSubs {
		bad := connectError(id, discBadRequest, errLimitExceeded, "limit exceeded")
		return connectDecision{}, &bad
	}
	for _, channel := range decision.channels {
		if _, code, message := config.ResolveChannel(n.cfg, channel); code != 0 {
			bad := connectError(id, discBadRequest, code, message)
			return connectDecision{}, &bad
		}
	}
	return decision, nil
}

func (n *Node) connectProxy(c *conn, id uint32, name, version string, data json.RawMessage) (connectProxyResult, *outcome) {
	payload := map[string]any{
		"client":    c.id,
		"transport": "websocket",
		"protocol":  "json",
		"encoding":  "json",
	}
	if name != "" {
		payload["name"] = name
	}
	if version != "" {
		payload["version"] = version
	}
	if len(data) > 0 && string(data) != "null" {
		payload["data"] = data
	}
	headers := map[string]string{}
	if c.cookie != "" {
		headers["cookie"] = c.cookie
	}
	if c.authorization != "" {
		headers["authorization"] = c.authorization
	}
	if len(headers) > 0 {
		payload["headers"] = headers
	}
	resp, err := n.postProxy(n.cfg.ConnectProxyEndpoint, proxyTimeout(n.cfg.ConnectProxyTimeoutMs), payload)
	if err != nil {
		bad := connectError(id, discUnavailable, errInternal, "internal server error")
		return connectProxyResult{}, &bad
	}
	if resp.Error != nil || resp.Disconnect != nil {
		bad := proxyConnectFailure(id, resp)
		return connectProxyResult{}, &bad
	}
	var result connectProxyResult
	if len(resp.Result) > 0 && string(resp.Result) != "null" {
		if err := json.Unmarshal(resp.Result, &result); err != nil {
			bad := connectError(id, discUnavailable, errInternal, "internal server error")
			return connectProxyResult{}, &bad
		}
	}
	return result, nil
}

func proxyConnectFailure(id uint32, resp proxyResponse) outcome {
	code := discUnauthorized
	reason := "unauthorized"
	errCode := errUnauthorized
	errMsg := "unauthorized"
	if resp.Error != nil {
		if resp.Error.Code != 0 {
			errCode = resp.Error.Code
		}
		if resp.Error.Message != "" {
			errMsg = resp.Error.Message
			reason = resp.Error.Message
		}
	}
	if resp.Disconnect != nil && resp.Disconnect.Code != 0 {
		code = resp.Disconnect.Code
		if resp.Disconnect.Reason != "" {
			reason = resp.Disconnect.Reason
		}
	}
	return connectError(id, code, errCode, errMsgOr(reason, errMsg))
}

func errMsgOr(reason, message string) string {
	if message != "" {
		return message
	}
	return reason
}

func (n *Node) finishConnect(c *conn, id uint32, decision connectDecision) outcome {
	if c.connectTimer != nil {
		c.connectTimer.Stop()
	}
	c.user = decision.user
	c.connInfo = decision.connInfo
	c.name = truncate(decision.name, 16)
	c.version = truncate(decision.version, 64)
	c.protocolConnected = true
	n.sessions[c.id] = c
	n.armPing(c)
	if decision.expireAt > time.Now().Unix() {
		c.expireTimer = time.AfterFunc(time.Until(time.Unix(decision.expireAt, 0)), func() {
			n.disconnect(c, discExpired, "expired")
		})
	}

	subs := map[string]any{}
	var effects []delivery
	for _, channel := range decision.channels {
		ns, _, _ := config.ResolveChannel(n.cfg, channel)
		hint := decision.hints[channel]
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
	if len(decision.data) > 0 && string(decision.data) != "null" {
		result["data"] = decision.data
	}
	return outcome{reply: map[string]any{"id": id, "connect": result}, effects: effects}
}

func (n *Node) subscribeCommand(c *conn, id uint32, body json.RawMessage) outcome {
	var req struct {
		Channel     string          `json:"channel"`
		Type        *int            `json:"type"`
		Recover     bool            `json:"recover"`
		Recoverable bool            `json:"recoverable"`
		Positioned  bool            `json:"positioned"`
		Epoch       string          `json:"epoch"`
		Offset      *uint64         `json:"offset"`
		Flag        *int64          `json:"flag"`
		Data        json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(body, &req); err != nil || req.Channel == "" {
		return outcome{reply: errorReply(id, errBadRequest, "bad request")}
	}
	if req.Type != nil && *req.Type != 0 {
		return outcome{reply: errorReply(id, errNotAvailable, "not available")}
	}
	n.mu.Lock()
	if c.dropped.Load() || !c.protocolConnected {
		n.mu.Unlock()
		return disconnectOutcome(discBadRequest, "bad request")
	}
	user := c.user
	n.mu.Unlock()
	ns, code, message := config.ResolveChannel(n.cfg, req.Channel)
	if code != 0 {
		return outcome{reply: errorReply(id, code, message)}
	}
	if !ns.AllowSubscribeForClient {
		return outcome{reply: errorReply(id, errPermissionDenied, "permission denied")}
	}
	var proxied *subscribeProxyResult
	if ns.SubscribeProxyEnabled {
		result, bad := n.subscribeProxy(c, id, user, req.Channel, req.Data)
		if bad != nil {
			return *bad
		}
		proxied = &result
		if proxied.Override.Presence != nil {
			ns.Presence = proxied.Override.Presence.Value
		}
		if proxied.Override.JoinLeave != nil {
			ns.JoinLeave = proxied.Override.JoinLeave.Value
		}
		if proxied.Override.ForceRecovery != nil {
			ns.ForceRecovery = proxied.Override.ForceRecovery.Value
		}
		if proxied.Override.ForcePositioning != nil {
			ns.ForcePositioning = proxied.Override.ForcePositioning.Value
		}
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if c.dropped.Load() || !c.protocolConnected {
		return outcome{}
	}
	return n.onSubscribe(c, id, req, ns, proxied)
}

func (n *Node) subscribeProxy(c *conn, id uint32, user, channel string, data json.RawMessage) (subscribeProxyResult, *outcome) {
	payload := map[string]any{
		"client":    c.id,
		"transport": "websocket",
		"protocol":  "json",
		"encoding":  "json",
		"user":      user,
		"channel":   channel,
	}
	if len(data) > 0 && string(data) != "null" {
		payload["data"] = data
	}
	resp, err := n.postProxy(n.cfg.SubscribeProxyEndpoint, proxyTimeout(n.cfg.SubscribeProxyTimeoutMs), payload)
	if err != nil {
		bad := outcome{reply: errorReply(id, errInternal, "internal server error")}
		return subscribeProxyResult{}, &bad
	}
	if resp.Disconnect != nil && resp.Disconnect.Code != 0 {
		reason := resp.Disconnect.Reason
		if reason == "" {
			reason = "disconnect"
		}
		errCode := errPermissionDenied
		errMsg := reason
		if resp.Error != nil {
			if resp.Error.Code != 0 {
				errCode = resp.Error.Code
			}
			if resp.Error.Message != "" {
				errMsg = resp.Error.Message
			}
		}
		bad := outcome{reply: errorReply(id, errCode, errMsg), disconnect: true, code: resp.Disconnect.Code, reason: reason}
		return subscribeProxyResult{}, &bad
	}
	if resp.Error != nil {
		code := resp.Error.Code
		if code == 0 {
			code = errPermissionDenied
		}
		msg := resp.Error.Message
		if msg == "" {
			msg = "permission denied"
		}
		bad := outcome{reply: errorReply(id, code, msg)}
		return subscribeProxyResult{}, &bad
	}
	var result subscribeProxyResult
	if len(resp.Result) > 0 && string(resp.Result) != "null" {
		if err := json.Unmarshal(resp.Result, &result); err != nil {
			bad := outcome{reply: errorReply(id, errInternal, "internal server error")}
			return subscribeProxyResult{}, &bad
		}
	}
	return result, nil
}

func (n *Node) onSubscribe(c *conn, id uint32, req struct {
	Channel     string          `json:"channel"`
	Type        *int            `json:"type"`
	Recover     bool            `json:"recover"`
	Recoverable bool            `json:"recoverable"`
	Positioned  bool            `json:"positioned"`
	Epoch       string          `json:"epoch"`
	Offset      *uint64         `json:"offset"`
	Flag        *int64          `json:"flag"`
	Data        json.RawMessage `json:"data"`
}, ns config.NamespaceConfig, proxied *subscribeProxyResult) outcome {
	hint := attachHint{recover: req.Recover, recoverable: req.Recoverable, positioned: req.Positioned, epoch: req.Epoch}
	if req.Offset != nil {
		hint.offset = *req.Offset
	}
	if req.Flag != nil {
		hint.flag = *req.Flag
	}
	if proxied != nil {
		hint.chanInfo = proxied.Info
		hint.resultData = proxied.Data
	}
	result, effects, code, message := n.attachLocked(c, req.Channel, ns, hint)
	if code != 0 {
		return outcome{reply: errorReply(id, code, message)}
	}
	return outcome{reply: map[string]any{"id": id, "subscribe": result}, effects: effects}
}
