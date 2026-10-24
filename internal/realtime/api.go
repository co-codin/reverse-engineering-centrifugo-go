package realtime

import (
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"

	"channeld/internal/config"
	"channeld/internal/token"
)

type apiError struct {
	Code    int
	Message string
}

func (n *Node) publishAPI(body map[string]json.RawMessage) (map[string]any, *apiError) {
	channel, ns, data, skip, tags, key, err := n.readPublish(body)
	if err != nil {
		return nil, err
	}
	n.mu.Lock()
	outcome, effects := n.publishLocked(channel, ns, data, skip, nil, tags, key)
	n.mu.Unlock()
	deliver(effects)
	return positionResult(outcome), nil
}

func (n *Node) broadcastAPI(body map[string]json.RawMessage) (map[string]any, *apiError) {
	var channels []string
	if err := json.Unmarshal(body["channels"], &channels); err != nil || len(channels) == 0 || len(channels) > maxBroadcast {
		return nil, &apiError{errBadRequest, "bad request"}
	}
	data, err := decodeData(body)
	if err != nil {
		return nil, err
	}
	tags, err := readTags(body["tags"])
	if err != nil {
		return nil, err
	}
	key, err := readIdempotency(body["idempotency_key"])
	if err != nil {
		return nil, err
	}
	skip, err := optionalBool(body, "skip_history")
	if err != nil {
		return nil, err
	}
	responses := make([]any, 0, len(channels))
	for _, channel := range channels {
		ns, code, message := config.ResolveChannel(n.cfg, channel)
		if code != 0 {
			responses = append(responses, map[string]any{"error": map[string]any{"code": code, "message": message}})
			continue
		}
		n.mu.Lock()
		outcome, effects := n.publishLocked(channel, ns, data, skip, nil, tags, key)
		n.mu.Unlock()
		deliver(effects)
		responses = append(responses, map[string]any{"result": positionResult(outcome)})
	}
	return map[string]any{"responses": responses}, nil
}

func (n *Node) presenceAPI(body map[string]json.RawMessage) (map[string]any, *apiError) {
	channel, ns, err := n.requireChannel(body)
	if err != nil {
		return nil, err
	}
	if !ns.Presence {
		return nil, &apiError{errNotAvailable, "not available"}
	}
	n.mu.Lock()
	presence := n.wirePresenceLocked(channel)
	n.mu.Unlock()
	return map[string]any{"presence": presence}, nil
}

func (n *Node) presenceStatsAPI(body map[string]json.RawMessage) (map[string]any, *apiError) {
	channel, ns, err := n.requireChannel(body)
	if err != nil {
		return nil, err
	}
	if !ns.Presence {
		return nil, &apiError{errNotAvailable, "not available"}
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	users := map[string]struct{}{}
	set := n.byChannel[channel]
	for _, member := range set {
		users[member.user] = struct{}{}
	}
	return map[string]any{"num_clients": len(set), "num_users": len(users)}, nil
}

func (n *Node) historyAPI(body map[string]json.RawMessage) (map[string]any, *apiError) {
	channel, ns, err := n.requireChannel(body)
	if err != nil {
		return nil, err
	}
	if ns.HistorySize <= 0 {
		return nil, &apiError{errNotAvailable, "not available"}
	}
	encoded, _ := json.Marshal(rawMap(body))
	query, code, message := parseHistoryBody(encoded)
	if code != 0 {
		return nil, &apiError{code, message}
	}
	n.mu.Lock()
	history := n.engine.History(channel, streamOptions(ns), query, time.Now().UnixMilli())
	n.mu.Unlock()
	if !history.OK {
		return nil, &apiError{history.Code, history.Message}
	}
	return historyResult(history), nil
}

func (n *Node) historyRemoveAPI(body map[string]json.RawMessage) (map[string]any, *apiError) {
	channel, _, err := n.requireChannel(body)
	if err != nil {
		return nil, err
	}
	n.mu.Lock()
	n.engine.RemoveHistory(channel)
	n.mu.Unlock()
	return map[string]any{}, nil
}

func (n *Node) channelsAPI(body map[string]json.RawMessage) (map[string]any, *apiError) {
	pattern := ""
	if raw, ok := body["pattern"]; ok && string(raw) != "null" {
		if err := json.Unmarshal(raw, &pattern); err != nil {
			return nil, &apiError{errBadRequest, "bad request"}
		}
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	channels := map[string]any{}
	for channel, set := range n.byChannel {
		if pattern != "" && !config.Glob(pattern, channel) {
			continue
		}
		channels[channel] = map[string]any{"num_clients": len(set)}
	}
	return map[string]any{"channels": channels}, nil
}

func (n *Node) infoAPI() map[string]any {
	n.mu.Lock()
	defer n.mu.Unlock()
	users := map[string]struct{}{}
	for _, c := range n.sessions {
		users[c.user] = struct{}{}
	}
	return map[string]any{"nodes": []any{map[string]any{
		"uid":          n.uid,
		"name":         n.cfg.NodeName,
		"version":      n.cfg.Version,
		"num_clients":  len(n.sessions),
		"num_users":    len(users),
		"num_channels": len(n.byChannel),
		"uptime":       int(time.Since(n.started).Seconds()),
	}}}
}

func (n *Node) disconnectAPI(body map[string]json.RawMessage) (map[string]any, *apiError) {
	user, clientID, session, whitelist, err := readTarget(body)
	if err != nil {
		return nil, err
	}
	code, reason, err := readDisconnect(body["disconnect"])
	if err != nil {
		return nil, err
	}
	n.mu.Lock()
	targets := n.matchLocked(user, clientID, session, whitelist)
	n.mu.Unlock()
	for _, c := range targets {
		n.disconnect(c, code, reason)
	}
	return map[string]any{}, nil
}

func (n *Node) subscribeAPI(body map[string]json.RawMessage) (map[string]any, *apiError) {
	user, clientID, session, whitelist, err := readTarget(body)
	if err != nil {
		return nil, err
	}
	channel, ns, err := n.requireChannel(body)
	if err != nil {
		return nil, err
	}
	hint := attachHint{}
	if info, ok := body["info"]; ok && string(info) != "null" {
		hint.chanInfo = info
	}
	if data, ok := body["data"]; ok && string(data) != "null" {
		hint.resultData = data
	}
	n.mu.Lock()
	var effects []delivery
	for _, c := range n.matchLocked(user, clientID, session, whitelist) {
		if _, ok := c.subs[channel]; ok {
			continue
		}
		result, joins, code, _ := n.attachLocked(c, channel, ns, hint)
		if code != 0 || result == nil {
			continue
		}
		effects = append(effects, delivery{conn: c, payload: mustEncode([]any{map[string]any{"push": map[string]any{"channel": channel, "subscribe": result}}})})
		effects = append(effects, joins...)
	}
	n.mu.Unlock()
	deliver(effects)
	return map[string]any{}, nil
}

func (n *Node) unsubscribeAPI(body map[string]json.RawMessage) (map[string]any, *apiError) {
	user, clientID, session, whitelist, err := readTarget(body)
	if err != nil {
		return nil, err
	}
	channel, _, err := n.requireChannel(body)
	if err != nil {
		return nil, err
	}
	n.mu.Lock()
	var effects []delivery
	for _, c := range n.matchLocked(user, clientID, session, whitelist) {
		if _, ok := c.subs[channel]; !ok {
			continue
		}
		effects = append(effects, n.removeSubLocked(c, channel)...)
		effects = append(effects, delivery{conn: c, payload: mustEncode([]any{map[string]any{"push": map[string]any{
			"channel":     channel,
			"unsubscribe": map[string]any{"code": 2000, "reason": "server unsubscribe"},
		}}})})
	}
	n.mu.Unlock()
	deliver(effects)
	return map[string]any{}, nil
}

func (n *Node) issueToken(body map[string]json.RawMessage) (map[string]any, *apiError) {
	user, ok := jsonString(body["user"])
	if !ok || user == "" || len(user) > 128 || hasControl(user) {
		return nil, &apiError{errBadRequest, "bad request"}
	}
	var channels []string
	if raw, exists := body["channels"]; exists && string(raw) != "null" {
		if err := json.Unmarshal(raw, &channels); err != nil {
			return nil, &apiError{errBadRequest, "bad request"}
		}
		for _, channel := range channels {
			if _, code, message := config.ResolveChannel(n.cfg, channel); code != 0 {
				return nil, &apiError{code, message}
			}
		}
	}
	ttl := 3600
	if raw, exists := body["ttl"]; exists && string(raw) != "null" {
		if err := json.Unmarshal(raw, &ttl); err != nil || ttl < 60 || ttl > 86_400 {
			return nil, &apiError{errBadRequest, "bad request"}
		}
	}
	now := time.Now().Unix()
	claims := token.Claims{Sub: user, Iat: now, Exp: now + int64(ttl)}
	if info, exists := body["info"]; exists && string(info) != "null" {
		claims.Info = info
	}
	claims.Channels = channels
	signed, err := token.Sign(claims, n.cfg.TokenSecret)
	if err != nil {
		return nil, &apiError{errInternal, "internal server error"}
	}
	return map[string]any{"token": signed, "expire_at": now + int64(ttl)}, nil
}

func (n *Node) readPublish(body map[string]json.RawMessage) (string, config.NamespaceConfig, json.RawMessage, bool, map[string]string, string, *apiError) {
	channel, ns, err := n.requireChannel(body)
	if err != nil {
		return "", config.NamespaceConfig{}, nil, false, nil, "", err
	}
	data, err := decodeData(body)
	if err != nil {
		return "", config.NamespaceConfig{}, nil, false, nil, "", err
	}
	tags, err := readTags(body["tags"])
	if err != nil {
		return "", config.NamespaceConfig{}, nil, false, nil, "", err
	}
	key, err := readIdempotency(body["idempotency_key"])
	if err != nil {
		return "", config.NamespaceConfig{}, nil, false, nil, "", err
	}
	skip, err := optionalBool(body, "skip_history")
	if err != nil {
		return "", config.NamespaceConfig{}, nil, false, nil, "", err
	}
	return channel, ns, data, skip, tags, key, nil
}

func (n *Node) requireChannel(body map[string]json.RawMessage) (string, config.NamespaceConfig, *apiError) {
	channel, ok := jsonString(body["channel"])
	if !ok {
		return "", config.NamespaceConfig{}, &apiError{errBadRequest, "bad request"}
	}
	ns, code, message := config.ResolveChannel(n.cfg, channel)
	if code != 0 {
		return "", config.NamespaceConfig{}, &apiError{code, message}
	}
	return channel, ns, nil
}

func (n *Node) matchLocked(user, clientID, session string, whitelist []string) []*conn {
	var found []*conn
	for _, c := range n.sessions {
		if c.user != user || (clientID != "" && c.id != clientID) || (session != "" && c.session != session) || contains(whitelist, c.id) {
			continue
		}
		found = append(found, c)
	}
	return found
}

func deliver(effects []delivery) {
	for _, effect := range effects {
		effect.conn.write(true, effect.payload)
	}
}

func readTarget(body map[string]json.RawMessage) (string, string, string, []string, *apiError) {
	raw, ok := body["user"]
	if !ok {
		return "", "", "", nil, &apiError{errBadRequest, "bad request"}
	}
	var user string
	if err := json.Unmarshal(raw, &user); err != nil || len(user) > 128 {
		return "", "", "", nil, &apiError{errBadRequest, "bad request"}
	}
	clientID, err := optionalString(body, "client")
	if err != nil {
		return "", "", "", nil, err
	}
	session, err := optionalString(body, "session")
	if err != nil {
		return "", "", "", nil, err
	}
	var whitelist []string
	if listed, exists := body["whitelist"]; exists && string(listed) != "null" {
		if err := json.Unmarshal(listed, &whitelist); err != nil {
			return "", "", "", nil, &apiError{errBadRequest, "bad request"}
		}
	}
	return user, clientID, session, whitelist, nil
}

func readDisconnect(raw json.RawMessage) (int, string, *apiError) {
	if len(raw) == 0 || string(raw) == "null" {
		return discForce, "force disconnect", nil
	}
	var body struct {
		Code   int    `json:"code"`
		Reason string `json:"reason"`
	}
	if err := json.Unmarshal(raw, &body); err != nil || body.Code < 3000 || body.Code > 4999 || len(body.Reason) > 120 || body.Reason == "" {
		return 0, "", &apiError{errBadRequest, "bad request"}
	}
	return body.Code, body.Reason, nil
}

func decodeData(body map[string]json.RawMessage) (json.RawMessage, *apiError) {
	_, hasData := body["data"]
	binary, hasBinary := body["b64data"]
	if hasData == hasBinary {
		return nil, &apiError{errBadRequest, "bad request"}
	}
	if hasData {
		return body["data"], nil
	}
	var encoded string
	if err := json.Unmarshal(binary, &encoded); err != nil {
		return nil, &apiError{errBadRequest, "bad request"}
	}
	decoded, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil || !json.Valid(decoded) {
		return nil, &apiError{errBadRequest, "bad request"}
	}
	return decoded, nil
}

func readTags(raw json.RawMessage) (map[string]string, *apiError) {
	if len(raw) == 0 || string(raw) == "null" {
		return nil, nil
	}
	var tags map[string]string
	if err := json.Unmarshal(raw, &tags); err != nil || len(tags) > 32 {
		return nil, &apiError{errBadRequest, "bad request"}
	}
	for key, value := range tags {
		if key == "" || len(key) > 64 || len(value) > 256 {
			return nil, &apiError{errBadRequest, "bad request"}
		}
	}
	return tags, nil
}

func readIdempotency(raw json.RawMessage) (string, *apiError) {
	if len(raw) == 0 || string(raw) == "null" {
		return "", nil
	}
	var key string
	if err := json.Unmarshal(raw, &key); err != nil || key == "" || len(key) > 128 {
		return "", &apiError{errBadRequest, "bad request"}
	}
	return key, nil
}

func optionalBool(body map[string]json.RawMessage, key string) (bool, *apiError) {
	raw, ok := body[key]
	if !ok || string(raw) == "null" {
		return false, nil
	}
	var value bool
	if err := json.Unmarshal(raw, &value); err != nil {
		return false, &apiError{errBadRequest, "bad request"}
	}
	return value, nil
}

func optionalString(body map[string]json.RawMessage, key string) (string, *apiError) {
	raw, ok := body[key]
	if !ok || string(raw) == "null" {
		return "", nil
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return "", &apiError{errBadRequest, "bad request"}
	}
	return value, nil
}

func jsonString(raw json.RawMessage) (string, bool) {
	if len(raw) == 0 {
		return "", false
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return "", false
	}
	return value, true
}

func rawMap(body map[string]json.RawMessage) map[string]any {
	out := map[string]any{}
	for key, value := range body {
		out[key] = value
	}
	return out
}

func contains(list []string, value string) bool {
	for _, item := range list {
		if item == value {
			return true
		}
	}
	return false
}

func hasControl(value string) bool {
	for _, r := range value {
		if r <= 0x1f {
			return true
		}
	}
	return false
}

func normalizeBody(data []byte) (map[string]json.RawMessage, error) {
	if len(strings.TrimSpace(string(data))) == 0 {
		return map[string]json.RawMessage{}, nil
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil || raw == nil {
		return nil, io.ErrUnexpectedEOF
	}
	out := make(map[string]json.RawMessage, len(raw))
	for key, value := range raw {
		out[strings.ToLower(key)] = value
	}
	return out, nil
}

func errBody(code int, message string) map[string]any {
	return map[string]any{"error": map[string]any{"code": code, "message": message}}
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "content-type, x-api-key")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(body)
}
