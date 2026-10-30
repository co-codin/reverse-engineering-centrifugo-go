package realtime

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"

	"channeld/internal/config"
	"channeld/internal/token"
)

func testConfig(overrides func(*config.Config)) config.Config {
	cfg := config.Default()
	cfg.Port = 0
	cfg.PingIntervalMs = 60_000
	if overrides != nil {
		overrides(&cfg)
	}
	return cfg
}

func withServer(t *testing.T, cfg config.Config, fn func(port int)) {
	t.Helper()
	running, err := Start(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := running.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	fn(running.Port)
}

type wsClient struct {
	t      *testing.T
	conn   *websocket.Conn
	mu     sync.Mutex
	frames [][]byte
	notify chan struct{}
	closed chan struct{}
	code   int
}

func dial(t *testing.T, port int) *wsClient {
	t.Helper()
	conn, _, err := websocket.Dial(context.Background(), fmt.Sprintf("ws://127.0.0.1:%d/connection/websocket", port), nil)
	if err != nil {
		t.Fatal(err)
	}
	client := &wsClient{t: t, conn: conn, notify: make(chan struct{}, 8), closed: make(chan struct{})}
	go client.read()
	return client
}

func (c *wsClient) read() {
	defer close(c.closed)
	for {
		_, data, err := c.conn.Read(context.Background())
		if err != nil {
			c.code = int(websocket.CloseStatus(err))
			return
		}
		c.mu.Lock()
		c.frames = append(c.frames, append([]byte(nil), data...))
		c.mu.Unlock()
		select {
		case c.notify <- struct{}{}:
		default:
		}
	}
}

func (c *wsClient) send(v any) {
	c.t.Helper()
	var data []byte
	switch typed := v.(type) {
	case string:
		data = []byte(typed)
	default:
		var err error
		data, err = json.Marshal(v)
		if err != nil {
			c.t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := c.conn.Write(ctx, websocket.MessageText, data); err != nil {
		c.t.Fatal(err)
	}
}

func (c *wsClient) next() []frame {
	c.t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		c.mu.Lock()
		if len(c.frames) > 0 {
			data := c.frames[0]
			c.frames = c.frames[1:]
			c.mu.Unlock()
			return decodeFrames(c.t, data)
		}
		c.mu.Unlock()
		select {
		case <-c.notify:
		case <-c.closed:
			c.t.Fatal("connection closed")
		case <-deadline:
			c.t.Fatal("timed out waiting for a frame")
		}
	}
}

func (c *wsClient) waitClose() int {
	c.t.Helper()
	select {
	case <-c.closed:
		return c.code
	case <-time.After(2 * time.Second):
		c.t.Fatal("timed out waiting for close")
		return 0
	}
}

type frame struct {
	ID    uint32 `json:"id"`
	Error *struct {
		Code int `json:"code"`
	} `json:"error"`
	Connect     json.RawMessage `json:"connect"`
	Subscribe   json.RawMessage `json:"subscribe"`
	Publish     json.RawMessage `json:"publish"`
	Unsubscribe json.RawMessage `json:"unsubscribe"`
	Push        json.RawMessage `json:"push"`
	Refresh     json.RawMessage `json:"refresh"`
	SubRefresh  json.RawMessage `json:"sub_refresh"`
	RPC         json.RawMessage `json:"rpc"`
}

func decodeFrames(t *testing.T, data []byte) []frame {
	t.Helper()
	var frames []frame
	for _, line := range bytes.Split(data, []byte("\n")) {
		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			continue
		}
		var decoded frame
		if err := json.Unmarshal(line, &decoded); err != nil {
			t.Fatal(err)
		}
		frames = append(frames, decoded)
	}
	return frames
}

func connect(t *testing.T, port int, jwt string) (*wsClient, string) {
	t.Helper()
	client := dial(t, port)
	body := map[string]any{"name": "test"}
	if jwt != "" {
		body["token"] = jwt
	}
	client.send(map[string]any{"id": 1, "connect": body})
	frames := client.next()
	if len(frames) != 1 || len(frames[0].Connect) == 0 {
		t.Fatalf("connect reply %+v", frames)
	}
	var result struct {
		Client string `json:"client"`
		Pong   bool   `json:"pong"`
	}
	if err := json.Unmarshal(frames[0].Connect, &result); err != nil {
		t.Fatal(err)
	}
	if result.Client == "" || !result.Pong {
		t.Fatalf("connect result %+v", result)
	}
	return client, result.Client
}

func subscribe(t *testing.T, client *wsClient, channel string, id int, extra map[string]any) frame {
	t.Helper()
	body := map[string]any{"channel": channel}
	for key, value := range extra {
		body[key] = value
	}
	client.send(map[string]any{"id": id, "subscribe": body})
	frames := client.next()
	if len(frames) != 1 {
		t.Fatalf("subscribe frames %+v", frames)
	}
	return frames[0]
}

func post(t *testing.T, port int, method string, body any, key string) (int, map[string]any) {
	t.Helper()
	data, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("http://127.0.0.1:%d/api/%s", port, method), bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set("X-API-Key", key)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	payload, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatal(err)
	}
	return res.StatusCode, decoded
}

func TestTwoClientsPresenceAndPublish(t *testing.T) {
	withServer(t, testConfig(nil), func(port int) {
		alice, _ := connect(t, port, "")
		sub := subscribe(t, alice, "chat:lobby", 2, nil)
		var subscribed struct {
			Recoverable bool `json:"recoverable"`
		}
		if err := json.Unmarshal(sub.Subscribe, &subscribed); err != nil || !subscribed.Recoverable {
			t.Fatalf("subscribe %s", sub.Subscribe)
		}
		bob, bobID := connect(t, port, "")
		joined := make(chan frame, 1)
		go func() { joined <- alice.next()[0] }()
		subscribe(t, bob, "chat:lobby", 2, nil)
		join := <-joined
		var push struct {
			Join struct {
				Info struct {
					Client string `json:"client"`
				} `json:"info"`
			} `json:"join"`
		}
		if err := json.Unmarshal(join.Push, &push); err != nil || push.Join.Info.Client != bobID {
			t.Fatalf("join %s", join.Push)
		}
		alicePub := make(chan frame, 1)
		bobPub := make(chan frame, 1)
		go func() { alicePub <- alice.next()[0] }()
		go func() { bobPub <- bob.next()[0] }()
		status, body := post(t, port, "publish", map[string]any{"channel": "chat:lobby", "data": map[string]any{"text": "hi"}}, "")
		if status != 200 || body["result"].(map[string]any)["offset"].(float64) != 1 {
			t.Fatalf("publish %d %v", status, body)
		}
		for _, item := range []frame{<-alicePub, <-bobPub} {
			var pub struct {
				Pub struct {
					Data map[string]any `json:"data"`
				} `json:"pub"`
			}
			if err := json.Unmarshal(item.Push, &pub); err != nil || pub.Pub.Data["text"] != "hi" {
				t.Fatalf("pub %s", item.Push)
			}
		}
		_, presence := post(t, port, "presence", map[string]any{"channel": "chat:lobby"}, "")
		clients := presence["result"].(map[string]any)["presence"].(map[string]any)
		if len(clients) != 2 {
			t.Fatalf("presence %v", clients)
		}
		left := make(chan frame, 1)
		go func() { left <- alice.next()[0] }()
		bob.send(map[string]any{"id": 3, "unsubscribe": map[string]string{"channel": "chat:lobby"}})
		unsub := bob.next()[0]
		if string(unsub.Unsubscribe) != "{}" {
			t.Fatalf("unsubscribe %s", unsub.Unsubscribe)
		}
		leave := <-left
		var gone struct {
			Leave struct {
				Info struct {
					Client string `json:"client"`
				} `json:"info"`
			} `json:"leave"`
		}
		if err := json.Unmarshal(leave.Push, &gone); err != nil || gone.Leave.Info.Client != bobID {
			t.Fatalf("leave %s", leave.Push)
		}
	})
}

func TestNamespacePermissions(t *testing.T) {
	withServer(t, testConfig(nil), func(port int) {
		client, _ := connect(t, port, "")
		if subscribe(t, client, "chat:lobby", 2, nil).Error != nil {
			t.Fatal("subscribe chat")
		}
		client.send(map[string]any{"id": 3, "publish": map[string]any{"channel": "chat:lobby", "data": map[string]any{"text": "from alice"}}})
		push := client.next()[0]
		var pub struct {
			Pub struct {
				Info struct {
					User string `json:"user"`
				} `json:"info"`
			} `json:"pub"`
		}
		if err := json.Unmarshal(push.Push, &pub); err != nil || pub.Pub.Info.User != "" {
			t.Fatalf("publisher %s", push.Push)
		}
		if string(client.next()[0].Publish) != "{}" {
			t.Fatal("publish ack")
		}
		if subscribe(t, client, "news:desk", 4, nil).Error != nil {
			t.Fatal("subscribe news")
		}
		if code := subscribe(t, client, "news:desk", 5, nil).Error.Code; code != errAlreadySubscribed {
			t.Fatalf("second subscribe %d", code)
		}
		client.send(map[string]any{"id": 6, "publish": map[string]any{"channel": "news:desk", "data": map[string]any{"text": "nope"}}})
		if client.next()[0].Error.Code != errPermissionDenied {
			t.Fatal("news publish allowed")
		}
		if subscribe(t, client, "nope:room", 7, nil).Error.Code != errUnknownChannel {
			t.Fatal("unknown namespace")
		}
		if subscribe(t, client, "chat:lobby", 8, nil).Error.Code != errAlreadySubscribed {
			t.Fatal("chat already")
		}
	})
}

func TestHistoryRecovery(t *testing.T) {
	cfg := testConfig(func(cfg *config.Config) {
		ns := config.Namespace("")
		ns.HistorySize = 2
		cfg.Namespaces = []config.NamespaceConfig{ns}
	})
	withServer(t, cfg, func(port int) {
		client, _ := connect(t, port, "")
		first := subscribe(t, client, "room", 2, nil)
		var pos struct {
			Epoch string `json:"epoch"`
		}
		if err := json.Unmarshal(first.Subscribe, &pos); err != nil || pos.Epoch == "" {
			t.Fatal(first.Subscribe)
		}
		client.send(map[string]any{"id": 3, "unsubscribe": map[string]string{"channel": "room"}})
		client.next()
		post(t, port, "publish", map[string]any{"channel": "room", "data": map[string]any{"n": 1}}, "")
		recovered := subscribe(t, client, "room", 4, map[string]any{"recover": true, "epoch": pos.Epoch, "offset": 0})
		var result struct {
			Recovered    bool   `json:"recovered"`
			Offset       uint64 `json:"offset"`
			Publications []struct {
				Data   map[string]any `json:"data"`
				Offset uint64         `json:"offset"`
			} `json:"publications"`
		}
		if err := json.Unmarshal(recovered.Subscribe, &result); err != nil {
			t.Fatal(err)
		}
		if !result.Recovered || result.Offset != 0 || len(result.Publications) != 1 || result.Publications[0].Offset != 1 || result.Publications[0].Data["n"].(float64) != 1 {
			t.Fatalf("recovered %+v", result)
		}
		client.send(map[string]any{"id": 5, "unsubscribe": map[string]string{"channel": "room"}})
		client.next()
		post(t, port, "publish", map[string]any{"channel": "room", "data": map[string]any{"n": 2}}, "")
		post(t, port, "publish", map[string]any{"channel": "room", "data": map[string]any{"n": 3}}, "")
		gap := subscribe(t, client, "room", 6, map[string]any{"recover": true, "epoch": pos.Epoch, "offset": 0})
		var missed map[string]any
		if err := json.Unmarshal(gap.Subscribe, &missed); err != nil {
			t.Fatal(err)
		}
		if missed["was_recovering"] != true || missed["recovered"] != nil || missed["offset"].(float64) != 3 {
			t.Fatalf("gap %v", missed)
		}
	})
}

func TestBatchAndEarlyCommand(t *testing.T) {
	withServer(t, testConfig(nil), func(port int) {
		client := dial(t, port)
		client.send("{\"id\":1,\"connect\":{\"name\":\"test\"}}\n{\"id\":2,\"subscribe\":{\"channel\":\"chat:lobby\"}}")
		frames := client.next()
		if len(frames) != 2 || len(frames[0].Connect) == 0 || len(frames[1].Subscribe) == 0 {
			t.Fatalf("%+v", frames)
		}
		early := dial(t, port)
		early.send(map[string]any{"id": 1, "subscribe": map[string]string{"channel": "chat:lobby"}})
		if early.waitClose() != discBadRequest {
			t.Fatalf("close %d", early.code)
		}
	})
}

func TestHTTPAPI(t *testing.T) {
	withServer(t, testConfig(nil), func(port int) {
		client, _ := connect(t, port, "")
		subscribe(t, client, "chat:one", 2, nil)
		subscribe(t, client, "chat:two", 3, nil)
		_, listed := post(t, port, "channels", map[string]any{"pattern": "chat:*"}, "")
		channels := listed["result"].(map[string]any)["channels"].(map[string]any)
		if _, ok := channels["chat:one"]; !ok || len(channels) != 2 {
			t.Fatalf("channels %v", channels)
		}
		_, broadcast := post(t, port, "broadcast", map[string]any{"channels": []string{"chat:one", "missing:nope"}, "data": map[string]any{"v": 1}}, "")
		responses := broadcast["result"].(map[string]any)["responses"].([]any)
		if responses[0].(map[string]any)["result"].(map[string]any)["offset"].(float64) != 1 {
			t.Fatal(responses[0])
		}
		if responses[1].(map[string]any)["error"].(map[string]any)["code"].(float64) != errUnknownChannel {
			t.Fatal(responses[1])
		}
		_, first := post(t, port, "publish", map[string]any{"channel": "chat:fresh", "data": map[string]any{"v": 1}, "idempotency_key": "k"}, "")
		_, second := post(t, port, "publish", map[string]any{"channel": "chat:fresh", "data": map[string]any{"v": 2}, "idempotency_key": "k"}, "")
		if first["result"].(map[string]any)["offset"] != second["result"].(map[string]any)["offset"] {
			t.Fatalf("%v %v", first, second)
		}
		_, history := post(t, port, "history", map[string]any{"channel": "chat:fresh", "limit": 10}, "")
		if pubs := history["result"].(map[string]any)["publications"].([]any); len(pubs) != 1 {
			t.Fatalf("history %v", history)
		}
	})
	locked := testConfig(func(cfg *config.Config) {
		cfg.Insecure = false
		cfg.APIKey = "secret"
	})
	withServer(t, locked, func(port int) {
		status, _ := post(t, port, "info", map[string]any{}, "")
		if status != http.StatusUnauthorized {
			t.Fatalf("status %d", status)
		}
		status, body := post(t, port, "info", map[string]any{}, "secret")
		if status != http.StatusOK {
			t.Fatal(body)
		}
	})
}

func TestTokenAndDisconnect(t *testing.T) {
	withServer(t, testConfig(nil), func(port int) {
		_, issued := post(t, port, "token", map[string]any{"user": "alice", "info": map[string]any{"desk": "a"}}, "")
		jwt := issued["result"].(map[string]any)["token"].(string)
		alice, _ := connect(t, port, jwt)
		subscribe(t, alice, "chat:lobby", 2, nil)
		done := make(chan int, 1)
		go func() { done <- alice.waitClose() }()
		post(t, port, "disconnect", map[string]any{"user": "alice"}, "")
		if code := <-done; code != discForce {
			t.Fatalf("disconnect %d", code)
		}
		expired, err := token.Sign(token.Claims{Sub: "alice", Exp: time.Now().Add(-time.Minute).Unix()}, "dev-secret")
		if err != nil {
			t.Fatal(err)
		}
		client := dial(t, port)
		closed := make(chan int, 1)
		go func() { closed <- client.waitClose() }()
		client.send(map[string]any{"id": 1, "connect": map[string]any{"token": expired}})
		if client.next()[0].Error.Code != errTokenExpired {
			t.Fatal("expired token")
		}
		if code := <-closed; code != discExpired {
			t.Fatalf("expired close %d", code)
		}
	})
}

func TestNoPong(t *testing.T) {
	cfg := testConfig(func(cfg *config.Config) { cfg.PingIntervalMs = 120 })
	withServer(t, cfg, func(port int) {
		client, _ := connect(t, port, "")
		if client.waitClose() != discNoPong {
			t.Fatalf("close %d", client.code)
		}
	})
}
