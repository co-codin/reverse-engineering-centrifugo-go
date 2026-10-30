package realtime

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"channeld/internal/config"
	"channeld/internal/token"
)

func TestPublishProxy(t *testing.T) {
	var publishes int
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		publishes++
		w.Header().Set("Content-Type", "application/json")
		if body["channel"] == "chat:secret" {
			_, _ = w.Write([]byte(`{"error":{"code":403,"message":"permission denied"}}`))
			return
		}
		_, _ = w.Write([]byte(`{"result":{"data":{"text":"rewritten"},"skip_history":true}}`))
	}))
	closed := false
	t.Cleanup(func() {
		if !closed {
			proxy.Close()
		}
	})
	cfg := testConfig(func(cfg *config.Config) {
		cfg.PublishProxyEndpoint = proxy.URL
		for i := range cfg.Namespaces {
			if cfg.Namespaces[i].Name == "chat" {
				cfg.Namespaces[i].PublishProxyEnabled = true
			}
		}
	})
	withServer(t, cfg, func(port int) {
		client, _ := connect(t, port, "")
		if subscribe(t, client, "chat:lobby", 2, nil).Error != nil {
			t.Fatal("subscribe")
		}
		client.send(map[string]any{"id": 3, "publish": map[string]any{"channel": "news:desk", "data": map[string]any{"text": "no"}}})
		denied := client.next()[0]
		if denied.Error == nil || denied.Error.Code != errPermissionDenied || publishes != 0 {
			t.Fatalf("news %+v publishes=%d", denied, publishes)
		}
		client.send(map[string]any{"id": 4, "publish": map[string]any{"channel": "chat:lobby", "data": map[string]any{"text": "original"}}})
		push := client.next()[0]
		var pub struct {
			Pub struct {
				Data map[string]any `json:"data"`
			} `json:"pub"`
		}
		if err := json.Unmarshal(push.Push, &pub); err != nil || pub.Pub.Data["text"] != "rewritten" {
			t.Fatalf("push %s", push.Push)
		}
		if reply := client.next()[0]; reply.Error != nil || len(reply.Publish) == 0 {
			t.Fatalf("publish reply %+v", reply)
		}
		status, history := post(t, port, "history", map[string]any{"channel": "chat:lobby", "limit": 10}, "")
		publications, _ := history["result"].(map[string]any)["publications"].([]any)
		if status != 200 || len(publications) != 0 {
			t.Fatalf("history %d %v", status, history)
		}
		if subscribe(t, client, "chat:secret", 5, nil).Error != nil {
			t.Fatal("subscribe secret")
		}
		client.send(map[string]any{"id": 6, "publish": map[string]any{"channel": "chat:secret", "data": map[string]any{"text": "no"}}})
		secret := client.next()[0]
		if secret.Error == nil || secret.Error.Code != 403 {
			t.Fatalf("secret %+v", secret)
		}
		proxy.Close()
		closed = true
		client.send(map[string]any{"id": 7, "publish": map[string]any{"channel": "chat:lobby", "data": map[string]any{"text": "later"}}})
		failed := client.next()[0]
		if failed.Error == nil || failed.Error.Code != errInternal {
			t.Fatalf("down %+v", failed)
		}
		client.send(map[string]any{"id": 8, "rpc": map[string]any{"method": "echo", "data": map[string]any{"ok": true}}})
		echo := client.next()[0]
		if echo.Error != nil || echo.ID != 8 {
			t.Fatalf("echo %+v", echo)
		}
	})
}

func TestChannelTokenAndRefresh(t *testing.T) {
	var subscribes int
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		subscribes++
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"result":{"data":{"welcome":true}}}`))
	}))
	defer proxy.Close()
	cfg := testConfig(func(cfg *config.Config) {
		cfg.SubscribeProxyEndpoint = proxy.URL
		cfg.Namespaces = append(cfg.Namespaces, config.Namespace("room"))
		for i := range cfg.Namespaces {
			switch cfg.Namespaces[i].Name {
			case "chat":
				cfg.Namespaces[i].AllowSubscribeForClient = false
				cfg.Namespaces[i].SubscribeProxyEnabled = true
			case "room":
				cfg.Namespaces[i].SubscribeProxyEnabled = true
			}
		}
	})
	withServer(t, cfg, func(port int) {
		client, _ := connect(t, port, "")
		if denied := subscribe(t, client, "chat:secret", 2, nil); denied.Error == nil || denied.Error.Code != errPermissionDenied || subscribes != 0 {
			t.Fatalf("open subscribe %+v calls=%d", denied, subscribes)
		}
		signed, err := token.Sign(token.Claims{Channel: "chat:secret", Info: json.RawMessage(`{"role":"member"}`)}, cfg.TokenSecret)
		if err != nil {
			t.Fatal(err)
		}
		if allowed := subscribe(t, client, "chat:secret", 3, map[string]any{"token": signed}); allowed.Error != nil || subscribes != 0 {
			t.Fatalf("token subscribe %+v calls=%d", allowed, subscribes)
		}
		other, err := token.Sign(token.Claims{Sub: "bob", Channel: "chat:secret"}, cfg.TokenSecret)
		if err != nil {
			t.Fatal(err)
		}
		if mismatch := subscribe(t, client, "chat:other", 4, map[string]any{"token": other}); mismatch.Error == nil || mismatch.Error.Code != errPermissionDenied {
			t.Fatalf("mismatch %+v", mismatch)
		}
		room, err := token.Sign(token.Claims{Channel: "room:lobby"}, cfg.TokenSecret)
		if err != nil {
			t.Fatal(err)
		}
		if skipped := subscribe(t, client, "room:lobby", 5, map[string]any{"token": room}); skipped.Error != nil || subscribes != 0 {
			t.Fatalf("room token %+v calls=%d", skipped, subscribes)
		}
		if proxied := subscribe(t, client, "room:hall", 6, nil); proxied.Error != nil || subscribes != 1 {
			t.Fatalf("room proxy %+v calls=%d", proxied, subscribes)
		}

		status, minted := post(t, port, "token", map[string]any{"user": "alice", "channel": "chat:minted", "ttl": 60}, "")
		if status != 200 {
			t.Fatalf("mint %d %v", status, minted)
		}
		mintedToken, _ := minted["result"].(map[string]any)["token"].(string)
		checked, err := token.VerifySubscription(mintedToken, cfg.TokenSecret, time.Now())
		if err != nil || checked.Claims.Channel != "chat:minted" || checked.Claims.Sub != "alice" {
			t.Fatalf("minted %+v %v", checked, err)
		}
		if both, body := post(t, port, "token", map[string]any{"user": "alice", "channel": "chat:minted", "channels": []string{"chat:lobby"}}, ""); both != 200 || body["error"] == nil {
			t.Fatalf("both %d %v", both, body)
		}
	})
}

func TestConnectionRefresh(t *testing.T) {
	cfg := testConfig(nil)
	withServer(t, cfg, func(port int) {
		client := dial(t, port)
		short, err := token.Sign(token.Claims{Sub: "alice", Exp: time.Now().Add(30 * time.Second).Unix()}, cfg.TokenSecret)
		if err != nil {
			t.Fatal(err)
		}
		client.send(map[string]any{"id": 1, "connect": map[string]any{"token": short}})
		connected := client.next()[0]
		var session struct {
			Expires bool  `json:"expires"`
			TTL     int64 `json:"ttl"`
		}
		if err := json.Unmarshal(connected.Connect, &session); err != nil || !session.Expires || session.TTL < 1 || session.TTL > 30 {
			t.Fatalf("connect %s", connected.Connect)
		}
		longer, err := token.Sign(token.Claims{Sub: "alice", Exp: time.Now().Add(time.Hour).Unix()}, cfg.TokenSecret)
		if err != nil {
			t.Fatal(err)
		}
		client.send(map[string]any{"id": 2, "refresh": map[string]any{"token": longer}})
		refreshed := client.next()[0]
		var extended struct {
			Expires bool  `json:"expires"`
			TTL     int64 `json:"ttl"`
		}
		if err := json.Unmarshal(refreshed.Refresh, &extended); err != nil || !extended.Expires || extended.TTL < 60 {
			t.Fatalf("refresh %s", refreshed.Refresh)
		}
		bob, err := token.Sign(token.Claims{Sub: "bob", Exp: time.Now().Add(time.Hour).Unix()}, cfg.TokenSecret)
		if err != nil {
			t.Fatal(err)
		}
		client.send(map[string]any{"id": 3, "refresh": map[string]any{"token": bob}})
		if mismatch := client.next()[0]; mismatch.Error == nil || mismatch.Error.Code != errUnauthorized {
			t.Fatalf("mismatch %+v", mismatch)
		}
		next, err := token.Sign(token.Claims{Sub: "alice", Channel: "chat:secret", Exp: time.Now().Add(time.Hour).Unix()}, cfg.TokenSecret)
		if err != nil {
			t.Fatal(err)
		}
		if subscribe(t, client, "chat:secret", 4, map[string]any{"token": next}).Error != nil {
			t.Fatal("subscribe")
		}
		replacement, err := token.Sign(token.Claims{Sub: "alice", Channel: "chat:secret", Exp: time.Now().Add(2 * time.Hour).Unix()}, cfg.TokenSecret)
		if err != nil {
			t.Fatal(err)
		}
		client.send(map[string]any{"id": 5, "sub_refresh": map[string]any{"channel": "chat:secret", "token": replacement}})
		sub := client.next()[0]
		var subTTL struct {
			TTL int64 `json:"ttl"`
		}
		if err := json.Unmarshal(sub.SubRefresh, &subTTL); err != nil || subTTL.TTL < 3600 {
			t.Fatalf("sub_refresh %s", sub.SubRefresh)
		}
	})
}

func TestSubscriptionExpiry(t *testing.T) {
	cfg := testConfig(nil)
	withServer(t, cfg, func(port int) {
		client, _ := connect(t, port, "")
		exp := time.Now().Add(time.Second).Unix()
		if exp <= time.Now().Unix() {
			exp++
		}
		signed, err := token.Sign(token.Claims{Channel: "chat:lobby", Exp: exp}, cfg.TokenSecret)
		if err != nil {
			t.Fatal(err)
		}
		if subscribe(t, client, "chat:lobby", 2, map[string]any{"token": signed}).Error != nil {
			t.Fatal("subscribe")
		}
		push := client.next()[0]
		var gone struct {
			Unsubscribe struct {
				Code int `json:"code"`
			} `json:"unsubscribe"`
		}
		if err := json.Unmarshal(push.Push, &gone); err != nil || gone.Unsubscribe.Code != errTokenExpired {
			t.Fatalf("unsubscribe %s", push.Push)
		}
	})
}

func TestShutdownCode(t *testing.T) {
	running, err := Start(testConfig(nil))
	if err != nil {
		t.Fatal(err)
	}
	client, _ := connect(t, running.Port, "")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := running.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if code := client.waitClose(); code != discShutdown {
		t.Fatalf("close %d", code)
	}
}

func TestMetrics(t *testing.T) {
	withServer(t, testConfig(nil), func(port int) {
		before := getMetrics(t, port)
		if !strings.Contains(before, "channeld_connections 0\n") || !strings.Contains(before, "channeld_publications_total 0\n") {
			t.Fatalf("empty metrics:\n%s", before)
		}
		client, _ := connect(t, port, "")
		if subscribe(t, client, "chat:lobby", 2, nil).Error != nil {
			t.Fatal("subscribe")
		}
		status, _ := post(t, port, "publish", map[string]any{"channel": "chat:lobby", "data": map[string]any{"text": "hi"}}, "")
		if status != 200 {
			t.Fatalf("publish %d", status)
		}
		client.next()
		body := getMetrics(t, port)
		for _, line := range []string{
			"channeld_connections 1\n",
			"channeld_subscriptions 1\n",
			"channeld_publications_total 1\n",
			"channeld_proxy_requests_total{proxy=\"publish\",result=\"ok\"} 0\n",
		} {
			if !strings.Contains(body, line) {
				t.Fatalf("missing %s in\n%s", line, body)
			}
		}
	})
}

func getMetrics(t *testing.T, port int) string {
	t.Helper()
	res, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/metrics", port))
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}
	if res.StatusCode != 200 {
		t.Fatalf("metrics %d %s", res.StatusCode, body)
	}
	return string(body)
}
