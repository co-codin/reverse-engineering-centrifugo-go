package realtime

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/coder/websocket"

	"channeld/internal/config"
)

func TestOriginCheck(t *testing.T) {
	cfg := testConfig(func(cfg *config.Config) {
		cfg.AllowedOrigins = []string{"127.0.0.1:4327"}
	})
	withServer(t, cfg, func(port int) {
		_, _, err := websocket.Dial(context.Background(), fmt.Sprintf("ws://127.0.0.1:%d/connection/websocket", port), &websocket.DialOptions{
			HTTPHeader: http.Header{"Origin": []string{"http://evil.example"}},
		})
		if err == nil {
			t.Fatal("accepted a foreign origin")
		}
		conn, _, err := websocket.Dial(context.Background(), fmt.Sprintf("ws://127.0.0.1:%d/connection/websocket", port), &websocket.DialOptions{
			HTTPHeader: http.Header{"Origin": []string{"http://127.0.0.1:4327"}},
		})
		if err != nil {
			t.Fatal(err)
		}
		conn.Close(websocket.StatusNormalClosure, "")
	})
}

func TestConnectAndSubscribeProxy(t *testing.T) {
	var connects, subscribes int
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/connect":
			connects++
			_, _ = w.Write([]byte(`{"result":{"user":"56","channels":["chat:lobby"],"data":{"ok":true}}}`))
		case "/subscribe":
			subscribes++
			if body["channel"] == "chat:secret" {
				_, _ = w.Write([]byte(`{"error":{"code":403,"message":"permission denied"}}`))
				return
			}
			_, _ = w.Write([]byte(`{"result":{"info":{"role":"member"},"data":{"welcome":true}}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer proxy.Close()
	cfg := testConfig(func(cfg *config.Config) {
		cfg.Anonymous = false
		cfg.ConnectProxyEnabled = true
		cfg.ConnectProxyEndpoint = proxy.URL + "/connect"
		cfg.SubscribeProxyEndpoint = proxy.URL + "/subscribe"
		for i := range cfg.Namespaces {
			if cfg.Namespaces[i].Name == "chat" {
				cfg.Namespaces[i].SubscribeProxyEnabled = true
			}
		}
	})
	withServer(t, cfg, func(port int) {
		client := dial(t, port)
		client.send(map[string]any{"id": 1, "connect": map[string]any{"name": "test"}})
		frames := client.next()
		if len(frames) != 1 || frames[0].Error != nil {
			t.Fatalf("connect %+v", frames)
		}
		var connected struct {
			Data json.RawMessage            `json:"data"`
			Subs map[string]json.RawMessage `json:"subs"`
		}
		if err := json.Unmarshal(frames[0].Connect, &connected); err != nil {
			t.Fatal(err)
		}
		if string(connected.Data) != `{"ok":true}` || connected.Subs["chat:lobby"] == nil {
			t.Fatalf("connect result %s", frames[0].Connect)
		}
		if connects != 1 || subscribes != 0 {
			t.Fatalf("proxy calls connect=%d subscribe=%d", connects, subscribes)
		}
		denied := subscribe(t, client, "chat:secret", 2, nil)
		if denied.Error == nil || denied.Error.Code != 403 {
			t.Fatalf("secret %+v", denied)
		}
		allowed := subscribe(t, client, "chat:room", 3, nil)
		if allowed.Error != nil {
			t.Fatalf("room %s", allowed.Subscribe)
		}
		var sub struct {
			Data json.RawMessage `json:"data"`
		}
		if err := json.Unmarshal(allowed.Subscribe, &sub); err != nil || string(sub.Data) != `{"welcome":true}` {
			t.Fatalf("subscribe data %s", allowed.Subscribe)
		}
		if subscribes != 2 {
			t.Fatalf("subscribe calls %d", subscribes)
		}
	})
}
