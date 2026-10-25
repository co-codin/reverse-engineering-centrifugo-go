// Package realtime is a single-node Centrifugo-compatible server.
package realtime

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"

	"channeld/internal/config"
	"channeld/internal/engine"
)

const (
	errInternal          = 100
	errUnauthorized      = 101
	errUnknownChannel    = 102
	errPermissionDenied  = 103
	errMethodNotFound    = 104
	errAlreadySubscribed = 105
	errLimitExceeded     = 106
	errBadRequest        = 107
	errNotAvailable      = 108
	errTokenExpired      = 109
	errUnrecoverable     = 112
	discShutdown         = 3001
	discSlow             = 3008
	discExpired          = 3009
	discNoPong           = 3012
	discMessageSize      = 3013
	discForce            = 3500
	discBadRequest       = 3501
	discUnauthorized     = 3503
	discUnavailable      = 3004
	maxFrame             = 64 * 1024
	maxSubs              = 32
	maxHistoryLimit      = 100
	maxBroadcast         = 100
	connectTimeout       = 10 * time.Second
)

type subState struct {
	chanInfo json.RawMessage
}

type conn struct {
	id                string
	session           string
	user              string
	connInfo          json.RawMessage
	name              string
	version           string
	protocolConnected bool
	awaitingPong      bool
	dropped           atomic.Bool
	subs              map[string]subState
	ws                *websocket.Conn
	writeMu           sync.Mutex
	stop              chan struct{}
	connectTimer      *time.Timer
	expireTimer       *time.Timer
	cookie            string
	authorization     string
}

type delivery struct {
	conn    *conn
	payload []byte
}

type outcome struct {
	reply      any
	effects    []delivery
	disconnect bool
	code       int
	reason     string
}

// Node is one realtime process.
type Node struct {
	cfg        config.Config
	uid        string
	started    time.Time
	mu         sync.Mutex
	engine     *engine.Engine
	live       map[*conn]struct{}
	sessions   map[string]*conn
	byChannel  map[string]map[string]*conn
	stopping   bool
	httpClient *http.Client
}

// New builds an empty node.
func New(cfg config.Config) *Node {
	return &Node{
		cfg:        cfg,
		uid:        newID(),
		started:    time.Now(),
		engine:     engine.New(),
		live:       map[*conn]struct{}{},
		sessions:   map[string]*conn{},
		byChannel:  map[string]map[string]*conn{},
		httpClient: &http.Client{},
	}
}

func (n *Node) accept(ws *websocket.Conn, r *http.Request) {
	ws.SetReadLimit(maxFrame + 1)
	c := &conn{
		id:            newID(),
		session:       newID(),
		subs:          map[string]subState{},
		ws:            ws,
		stop:          make(chan struct{}),
		cookie:        r.Header.Get("Cookie"),
		authorization: r.Header.Get("Authorization"),
	}
	n.mu.Lock()
	n.live[c] = struct{}{}
	c.connectTimer = time.AfterFunc(connectTimeout, func() {
		n.mu.Lock()
		connected := c.protocolConnected
		n.mu.Unlock()
		if !connected {
			n.disconnect(c, discBadRequest, "connect timeout")
		}
	})
	n.mu.Unlock()
	n.readLoop(c)
}

func (n *Node) readLoop(c *conn) {
	defer n.drop(c)
	for {
		kind, data, err := c.ws.Read(context.Background())
		if err != nil {
			return
		}
		if kind != websocket.MessageText {
			n.disconnect(c, discBadRequest, "bad request")
			return
		}
		if len(data) > maxFrame {
			n.disconnect(c, discMessageSize, "message too large")
			return
		}
		if !n.onFrame(c, data) {
			return
		}
	}
}

func (n *Node) drop(c *conn) {
	n.mu.Lock()
	if !c.dropped.CompareAndSwap(false, true) {
		n.mu.Unlock()
		return
	}
	if c.connectTimer != nil {
		c.connectTimer.Stop()
	}
	if c.expireTimer != nil {
		c.expireTimer.Stop()
	}
	select {
	case <-c.stop:
	default:
		close(c.stop)
	}
	delete(n.live, c)
	delete(n.sessions, c.id)
	leaves := n.detachLocked(c)
	n.mu.Unlock()
	for _, leave := range leaves {
		leave.conn.write(true, leave.payload)
	}
}

func (n *Node) disconnect(c *conn, code int, reason string) {
	if len(reason) > 120 {
		reason = reason[:120]
	}
	payload := mustEncode([]any{map[string]any{"push": map[string]any{"disconnect": map[string]any{"code": code, "reason": reason}}}})
	n.drop(c)
	c.writeClose(payload, code, reason)
}

// Shutdown closes every connection.
func (n *Node) Shutdown() {
	n.mu.Lock()
	n.stopping = true
	all := make([]*conn, 0, len(n.live))
	for c := range n.live {
		all = append(all, c)
	}
	n.mu.Unlock()
	for _, c := range all {
		n.drop(c)
		c.ws.CloseNow()
	}
}

func (c *conn) write(respectDrop bool, payload []byte) {
	if respectDrop && c.dropped.Load() {
		return
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if respectDrop && c.dropped.Load() {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = c.ws.Write(ctx, websocket.MessageText, payload)
}

func (c *conn) writeClose(payload []byte, code int, reason string) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = c.ws.Write(ctx, websocket.MessageText, payload)
	_ = c.ws.Close(websocket.StatusCode(code), reason)
}

func (n *Node) armPing(c *conn) {
	if n.cfg.PingIntervalMs <= 0 {
		return
	}
	interval := time.Duration(n.cfg.PingIntervalMs) * time.Millisecond
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-c.stop:
				return
			case <-ticker.C:
				n.ping(c)
			}
		}
	}()
}

func (n *Node) ping(c *conn) {
	n.mu.Lock()
	if c.dropped.Load() || !c.protocolConnected {
		n.mu.Unlock()
		return
	}
	if c.awaitingPong {
		n.mu.Unlock()
		n.disconnect(c, discNoPong, "no pong")
		return
	}
	c.awaitingPong = true
	n.mu.Unlock()
	c.write(true, []byte("{}"))
}

func (n *Node) publishLocked(channel string, ns config.NamespaceConfig, data json.RawMessage, skip bool, info map[string]any, tags map[string]string, key string) (engine.PublishOutcome, []delivery) {
	outcome := n.engine.Publish(channel, streamOptions(ns), engine.PublishInput{
		Data:           data,
		SkipHistory:    skip,
		Info:           info,
		Tags:           tags,
		IdempotencyKey: key,
	}, time.Now().UnixMilli())
	if outcome.Duplicate || outcome.Publication == nil {
		return outcome, nil
	}
	payload := mustEncode([]any{map[string]any{"push": map[string]any{"channel": channel, "pub": outcome.Publication.Wire()}}})
	var out []delivery
	for _, member := range n.byChannel[channel] {
		out = append(out, delivery{conn: member, payload: payload})
	}
	return outcome, out
}

func (n *Node) attachLocked(c *conn, channel string, ns config.NamespaceConfig, hint attachHint) (map[string]any, []delivery, int, string) {
	if _, ok := c.subs[channel]; ok {
		return nil, nil, errAlreadySubscribed, "already subscribed"
	}
	if len(c.subs) >= maxSubs {
		return nil, nil, errLimitExceeded, "limit exceeded"
	}
	history := ns.HistorySize > 0
	recoverable := history && (ns.ForceRecovery || hint.recoverable)
	positioned := history && (ns.ForcePositioning || hint.positioned)
	opts := streamOptions(ns)
	now := time.Now().UnixMilli()
	result := map[string]any{}
	if history {
		if recoverable && hint.recover {
			recovered := n.engine.Recover(channel, opts, hint.epoch, hint.offset, now)
			if !recovered.Recovered && hint.flag&2 != 0 {
				return nil, nil, errUnrecoverable, "unrecoverable position"
			}
			result["recoverable"] = recoverable
			result["positioned"] = positioned
			result["epoch"] = recovered.Epoch
			result["offset"] = recovered.Offset
			result["was_recovering"] = true
			if recovered.Recovered {
				result["recovered"] = true
				if len(recovered.Publications) > 0 {
					pubs := make([]any, len(recovered.Publications))
					for i, pub := range recovered.Publications {
						pubs[i] = pub.Wire()
					}
					result["publications"] = pubs
				}
			}
		} else {
			epoch, offset := n.engine.Position(channel, opts, now)
			result["recoverable"] = recoverable
			result["positioned"] = positioned
			result["epoch"] = epoch
			result["offset"] = offset
		}
	}
	if len(hint.resultData) > 0 {
		result["data"] = hint.resultData
	}
	c.subs[channel] = subState{chanInfo: hint.chanInfo}
	set := n.byChannel[channel]
	if set == nil {
		set = map[string]*conn{}
		n.byChannel[channel] = set
	}
	set[c.id] = c
	return result, n.joinLeaveLocked(channel, "join", c, hint.chanInfo), 0, ""
}

type attachHint struct {
	recover     bool
	recoverable bool
	positioned  bool
	epoch       string
	offset      uint64
	flag        int64
	chanInfo    json.RawMessage
	resultData  json.RawMessage
}

func (n *Node) removeSubLocked(c *conn, channel string) []delivery {
	sub, ok := c.subs[channel]
	if !ok {
		return nil
	}
	delete(c.subs, channel)
	if set := n.byChannel[channel]; set != nil {
		delete(set, c.id)
		if len(set) == 0 {
			delete(n.byChannel, channel)
		}
	}
	return n.joinLeaveLocked(channel, "leave", c, sub.chanInfo)
}

func (n *Node) detachLocked(c *conn) []delivery {
	var out []delivery
	for channel := range c.subs {
		out = append(out, n.removeSubLocked(c, channel)...)
	}
	return out
}

func (n *Node) joinLeaveLocked(channel, kind string, c *conn, chanInfo json.RawMessage) []delivery {
	if n.stopping {
		return nil
	}
	ns, code, _ := config.ResolveChannel(n.cfg, channel)
	if code != 0 || !ns.Presence || !ns.JoinLeave {
		return nil
	}
	payload := mustEncode([]any{map[string]any{"push": map[string]any{
		"channel": channel,
		kind:      map[string]any{"info": wireInfo(c, chanInfo)},
	}}})
	var out []delivery
	for id, other := range n.byChannel[channel] {
		if id == c.id {
			continue
		}
		out = append(out, delivery{conn: other, payload: payload})
	}
	return out
}

func (n *Node) wirePresenceLocked(channel string) map[string]any {
	presence := map[string]any{}
	for _, member := range n.byChannel[channel] {
		presence[member.id] = wireInfo(member, member.subs[channel].chanInfo)
	}
	return presence
}

func wireInfo(c *conn, chanInfo json.RawMessage) map[string]any {
	info := map[string]any{"user": c.user, "client": c.id}
	if len(c.connInfo) > 0 {
		info["conn_info"] = c.connInfo
	}
	if len(chanInfo) > 0 {
		info["chan_info"] = chanInfo
	}
	return info
}

func streamOptions(ns config.NamespaceConfig) engine.StreamOptions {
	return engine.StreamOptions{HistorySize: ns.HistorySize, HistoryTTLMs: ns.HistoryTTLMs}
}

func positionResult(outcome engine.PublishOutcome) map[string]any {
	result := map[string]any{}
	if outcome.HasEpoch {
		result["epoch"] = outcome.Epoch
	}
	if outcome.HasOffset {
		result["offset"] = outcome.Offset
	}
	return result
}

func historyResult(h engine.HistoryOutcome) map[string]any {
	result := map[string]any{"epoch": h.Epoch, "offset": h.Offset}
	if len(h.Publications) > 0 {
		pubs := make([]any, len(h.Publications))
		for i, pub := range h.Publications {
			pubs[i] = pub.Wire()
		}
		result["publications"] = pubs
	}
	return result
}

func mustEncode(frames []any) []byte {
	data, err := encodeFrames(frames)
	if err != nil {
		panic(err)
	}
	return data
}

func encodeFrames(frames []any) ([]byte, error) {
	var buf []byte
	for i, frame := range frames {
		if i > 0 {
			buf = append(buf, '\n')
		}
		data, err := json.Marshal(frame)
		if err != nil {
			return nil, err
		}
		buf = append(buf, data...)
	}
	return buf, nil
}

func newID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

func truncate(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n])
}
