package config

import "testing"

func TestDurationAndInheritance(t *testing.T) {
	if ms, ok := ParseDuration("5m"); !ok || ms != 300_000 {
		t.Fatalf("5m = %d %v", ms, ok)
	}
	if ms, ok := ParseDuration("25s"); !ok || ms != 25_000 {
		t.Fatalf("25s = %d %v", ms, ok)
	}
	cfg, err := Parse([]byte(`{
		"channel": {
			"allow_publish_for_subscriber": true,
			"namespaces": [{ "name": "news", "allow_publish_for_subscriber": false }]
		}
	}`))
	if err != nil {
		t.Fatal(err)
	}
	var blank, news *NamespaceConfig
	for i := range cfg.Namespaces {
		switch cfg.Namespaces[i].Name {
		case "":
			blank = &cfg.Namespaces[i]
		case "news":
			news = &cfg.Namespaces[i]
		}
	}
	if blank == nil || !blank.AllowPublishForSubscriber || news == nil || news.AllowPublishForSubscriber || news.HistorySize != 100 {
		t.Fatalf("%+v", cfg.Namespaces)
	}
	shipped, err := Load("../../configs/channeld.json")
	if err != nil {
		t.Fatal(err)
	}
	if shipped.Port != 4319 || !shipped.Insecure || len(shipped.AllowedOrigins) != 2 {
		t.Fatalf("%+v", shipped)
	}
	foundChat := false
	for _, ns := range shipped.Namespaces {
		if ns.Name == "chat" {
			foundChat = true
		}
	}
	if !foundChat {
		t.Fatal("missing chat namespace")
	}
}

func TestResolveChannelAndGlob(t *testing.T) {
	cfg := Default()
	ns, code, _ := ResolveChannel(cfg, "chat:lobby")
	if code != 0 || ns.Name != "chat" || !ns.AllowPublishForSubscriber {
		t.Fatalf("%+v %d", ns, code)
	}
	plain, code, _ := ResolveChannel(cfg, "lobby")
	if code != 0 || plain.Name != "" {
		t.Fatalf("%+v %d", plain, code)
	}
	news, code, _ := ResolveChannel(cfg, "news:desk")
	if code != 0 || news.AllowPublishForSubscriber || news.AllowPublishForClient {
		t.Fatalf("%+v %d", news, code)
	}
	if _, code, msg := ResolveChannel(cfg, "missing:room"); code != 102 || msg != "unknown channel" {
		t.Fatalf("%d %s", code, msg)
	}
	for _, name := range []string{"", "bad name", ":room", "room:"} {
		if _, code, _ := ResolveChannel(cfg, name); code != 107 {
			t.Fatalf("%q -> %d", name, code)
		}
	}
	if !Glob("", "anything") || !Glob("chat:*", "chat:lobby") || !Glob("chat:?", "chat:a") {
		t.Fatal("expected matches")
	}
	if Glob("chat:*", "news:lobby") || Glob("chat:?", "chat:lobby") {
		t.Fatal("unexpected match")
	}
}

func TestRejectsBadConfig(t *testing.T) {
	if _, err := Parse([]byte(`{"port":70000}`)); err == nil {
		t.Fatal("accepted an out of range port")
	}
	if _, err := Parse([]byte(`{"channel":{"namespaces":[{"name":"bad:name"}]}}`)); err == nil {
		t.Fatal("accepted a namespace name containing a colon")
	}
	if _, err := Parse([]byte(`{"client":{"allowed_origins":["http://evil.example"]}}`)); err == nil {
		t.Fatal("accepted an origin URL")
	}
	if _, err := Parse([]byte(`{"client":{"proxy":{"connect":{"enabled":true}}}}`)); err == nil {
		t.Fatal("accepted a connect proxy without an endpoint")
	}
	cfg, err := Parse([]byte(`{
		"client": {"proxy": {"connect": {"enabled": true, "endpoint": "http://app/connect", "timeout": "1s"}}},
		"channel": {
			"proxy": {"subscribe": {"endpoint": "http://app/subscribe"}},
			"namespaces": [{"name": "chat", "subscribe_proxy_enabled": true}]
		}
	}`))
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.ConnectProxyEnabled || cfg.ConnectProxyEndpoint != "http://app/connect" || cfg.SubscribeProxyEndpoint != "http://app/subscribe" {
		t.Fatalf("%+v", cfg)
	}
	var chat *NamespaceConfig
	for i := range cfg.Namespaces {
		if cfg.Namespaces[i].Name == "chat" {
			chat = &cfg.Namespaces[i]
		}
	}
	if chat == nil || !chat.SubscribeProxyEnabled {
		t.Fatal("chat subscribe proxy")
	}
}
