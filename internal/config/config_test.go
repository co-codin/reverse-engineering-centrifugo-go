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
	if shipped.Port != 4319 || !shipped.Insecure {
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
