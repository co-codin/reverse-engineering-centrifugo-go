// Package config loads channeld.json.
package config

import (
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"strings"
)

const Version = "0.1.0"

// NamespaceConfig is the policy for one channel prefix.
type NamespaceConfig struct {
	Name                      string
	Presence                  bool
	JoinLeave                 bool
	HistorySize               int
	HistoryTTLMs              int64
	AllowSubscribeForClient   bool
	AllowPublishForSubscriber bool
	AllowPublishForClient     bool
	ForceRecovery             bool
	ForcePositioning          bool
	SubscribeProxyEnabled     bool
}

// Config is the process configuration.
type Config struct {
	Host                    string
	Port                    int
	APIKey                  string
	Insecure                bool
	TokenSecret             string
	Anonymous               bool
	PingIntervalMs          int64
	AllowedOrigins          []string
	ConnectProxyEnabled     bool
	ConnectProxyEndpoint    string
	ConnectProxyTimeoutMs   int64
	SubscribeProxyEndpoint  string
	SubscribeProxyTimeoutMs int64
	NodeName                string
	Version                 string
	Namespaces              []NamespaceConfig
}

// Default is the configuration used when channeld.json is absent.
func Default() Config {
	return Config{
		Host:                    "0.0.0.0",
		Port:                    4319,
		APIKey:                  "dev-api-key",
		Insecure:                true,
		TokenSecret:             "dev-secret",
		Anonymous:               true,
		PingIntervalMs:          25_000,
		ConnectProxyTimeoutMs:   1000,
		SubscribeProxyTimeoutMs: 1000,
		NodeName:                "channeld",
		Version:                 Version,
		Namespaces: []NamespaceConfig{
			Namespace(""),
			Namespace("chat"),
			func() NamespaceConfig {
				ns := Namespace("news")
				ns.AllowPublishForSubscriber = false
				ns.AllowPublishForClient = false
				return ns
			}(),
		},
	}
}

// Namespace returns a namespace with the default channel policy.
func Namespace(name string) NamespaceConfig {
	return NamespaceConfig{
		Name:                      name,
		Presence:                  true,
		JoinLeave:                 true,
		HistorySize:               100,
		HistoryTTLMs:              5 * 60_000,
		AllowSubscribeForClient:   true,
		AllowPublishForSubscriber: true,
		ForceRecovery:             true,
		ForcePositioning:          true,
	}
}

// Load reads a config file. A missing file returns Default.
func Load(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return Default(), nil
		}
		return Config{}, err
	}
	return Parse(data)
}

// Parse decodes a config document.
func Parse(data []byte) (Config, error) {
	var top map[string]json.RawMessage
	if err := json.Unmarshal(data, &top); err != nil {
		return Config{}, fmt.Errorf("config must be a JSON object")
	}
	cfg := Default()
	if raw, ok := top["port"]; ok {
		port, err := parseInt(raw, "port")
		if err != nil || port < 0 || port > 65535 {
			return Config{}, fmt.Errorf("port must be an integer from 0 to 65535")
		}
		cfg.Port = port
	}
	if raw, ok := top["host"]; ok {
		host, err := parseString(raw, "host")
		if err != nil || host == "" {
			return Config{}, fmt.Errorf("host must be a string")
		}
		cfg.Host = host
	}
	if raw, ok := top["node_name"]; ok {
		name, err := parseString(raw, "node_name")
		if err != nil || name == "" {
			return Config{}, fmt.Errorf("node_name must be a string")
		}
		cfg.NodeName = name
	}
	if raw, ok := top["http_api"]; ok {
		fields, err := object(raw, "http_api")
		if err != nil {
			return Config{}, err
		}
		if key, ok := fields["key"]; ok {
			value, err := parseString(key, "http_api.key")
			if err != nil {
				return Config{}, fmt.Errorf("http_api.key must be a string")
			}
			cfg.APIKey = value
		}
		if insecure, ok := fields["insecure"]; ok {
			value, err := parseBool(insecure, "http_api.insecure")
			if err != nil {
				return Config{}, fmt.Errorf("http_api.insecure must be a boolean")
			}
			cfg.Insecure = value
		}
	}
	if raw, ok := top["client"]; ok {
		fields, err := object(raw, "client")
		if err != nil {
			return Config{}, err
		}
		if secret, ok := fields["token_hmac_secret"]; ok {
			value, err := parseString(secret, "client.token_hmac_secret")
			if err != nil || value == "" {
				return Config{}, fmt.Errorf("client.token_hmac_secret must be a non-empty string")
			}
			cfg.TokenSecret = value
		}
		if anonymous, ok := fields["anonymous"]; ok {
			value, err := parseBool(anonymous, "client.anonymous")
			if err != nil {
				return Config{}, fmt.Errorf("client.anonymous must be a boolean")
			}
			cfg.Anonymous = value
		}
		if ping, ok := fields["ping_interval"]; ok {
			text, err := parseString(ping, "client.ping_interval")
			if err != nil {
				return Config{}, fmt.Errorf("client.ping_interval must be a duration string")
			}
			ms, ok := ParseDuration(text)
			if !ok {
				return Config{}, fmt.Errorf("invalid ping_interval %q", text)
			}
			cfg.PingIntervalMs = ms
		}
		if raw, ok := fields["allowed_origins"]; ok {
			hosts, err := parseHosts(raw)
			if err != nil {
				return Config{}, fmt.Errorf("client.allowed_origins: %w", err)
			}
			cfg.AllowedOrigins = hosts
		}
		if raw, ok := fields["proxy"]; ok {
			proxy, err := object(raw, "client.proxy")
			if err != nil {
				return Config{}, err
			}
			if connect, ok := proxy["connect"]; ok {
				fields, err := object(connect, "client.proxy.connect")
				if err != nil {
					return Config{}, err
				}
				if enabled, ok := fields["enabled"]; ok {
					value, err := parseBool(enabled, "client.proxy.connect.enabled")
					if err != nil {
						return Config{}, err
					}
					cfg.ConnectProxyEnabled = value
				}
				if endpoint, ok := fields["endpoint"]; ok {
					value, err := parseString(endpoint, "client.proxy.connect.endpoint")
					if err != nil || (value == "" && cfg.ConnectProxyEnabled) {
						return Config{}, fmt.Errorf("client.proxy.connect.endpoint must be a URL")
					}
					cfg.ConnectProxyEndpoint = value
				}
				if timeout, ok := fields["timeout"]; ok {
					ms, err := parseTimeout(timeout, "client.proxy.connect.timeout")
					if err != nil {
						return Config{}, err
					}
					cfg.ConnectProxyTimeoutMs = ms
				}
			}
		}
		if cfg.ConnectProxyEnabled && cfg.ConnectProxyEndpoint == "" {
			return Config{}, fmt.Errorf("client.proxy.connect.endpoint must be a URL")
		}
	}

	channelRaw := json.RawMessage(`{}`)
	if raw, ok := top["channel"]; ok {
		channelRaw = raw
	}
	channel, err := object(channelRaw, "channel")
	if err != nil {
		return Config{}, err
	}
	delete(channel, "namespaces")
	if proxyRaw, ok := channel["proxy"]; ok {
		delete(channel, "proxy")
		proxy, err := object(proxyRaw, "channel.proxy")
		if err != nil {
			return Config{}, err
		}
		if subscribe, ok := proxy["subscribe"]; ok {
			fields, err := object(subscribe, "channel.proxy.subscribe")
			if err != nil {
				return Config{}, err
			}
			if endpoint, ok := fields["endpoint"]; ok {
				value, err := parseString(endpoint, "channel.proxy.subscribe.endpoint")
				if err != nil {
					return Config{}, fmt.Errorf("channel.proxy.subscribe.endpoint must be a URL")
				}
				cfg.SubscribeProxyEndpoint = value
			}
			if timeout, ok := fields["timeout"]; ok {
				ms, err := parseTimeout(timeout, "channel.proxy.subscribe.timeout")
				if err != nil {
					return Config{}, err
				}
				cfg.SubscribeProxyTimeoutMs = ms
			}
		}
	}
	base := namespaceDefaults()
	defaults, err := applyNamespace(channel, base, "")
	if err != nil {
		return Config{}, err
	}
	namespaces := []NamespaceConfig{defaults}
	if listed, ok := objectField(channelRaw, "namespaces"); ok {
		var items []json.RawMessage
		if err := json.Unmarshal(listed, &items); err != nil {
			return Config{}, fmt.Errorf("channel.namespaces must be an array")
		}
		inherited := defaults
		inherited.Name = ""
		for _, item := range items {
			fields, err := object(item, "namespace")
			if err != nil {
				return Config{}, fmt.Errorf("each namespace needs a name")
			}
			nameRaw, ok := fields["name"]
			name, err := parseString(nameRaw, "namespace name")
			if !ok || err != nil || name == "" {
				return Config{}, fmt.Errorf("each namespace needs a name")
			}
			if strings.Contains(name, ":") {
				return Config{}, fmt.Errorf("namespace name %q cannot contain ':'", name)
			}
			ns, err := applyNamespace(fields, inherited, name)
			if err != nil {
				return Config{}, err
			}
			namespaces = append(namespaces, ns)
		}
	}
	cfg.Namespaces = namespaces
	cfg.Version = Version
	for _, ns := range namespaces {
		if ns.SubscribeProxyEnabled && cfg.SubscribeProxyEndpoint == "" {
			name := ns.Name
			if name == "" {
				name = "(default)"
			}
			return Config{}, fmt.Errorf("namespace %s enables subscribe proxy but channel.proxy.subscribe.endpoint is empty", name)
		}
	}
	return cfg, nil
}

func objectField(raw json.RawMessage, key string) (json.RawMessage, bool) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return nil, false
	}
	value, ok := fields[key]
	return value, ok
}

func namespaceDefaults() NamespaceConfig {
	ns := Namespace("")
	ns.Name = ""
	return ns
}

func applyNamespace(raw map[string]json.RawMessage, base NamespaceConfig, name string) (NamespaceConfig, error) {
	next := base
	next.Name = name
	label := name
	if label == "" {
		label = "(default)"
	}
	var err error
	if next.Presence, err = overrideBool(raw, "presence", next.Presence, label); err != nil {
		return NamespaceConfig{}, err
	}
	if next.JoinLeave, err = overrideBool(raw, "join_leave", next.JoinLeave, label); err != nil {
		return NamespaceConfig{}, err
	}
	if next.HistorySize, err = overrideInt(raw, "history_size", next.HistorySize, label); err != nil {
		return NamespaceConfig{}, err
	}
	if rawTTL, ok := raw["history_ttl"]; ok {
		text, err := parseString(rawTTL, "history_ttl")
		if err != nil {
			return NamespaceConfig{}, fmt.Errorf("namespace %s: history_ttl must be a duration string", label)
		}
		ms, ok := ParseDuration(text)
		if !ok {
			return NamespaceConfig{}, fmt.Errorf("namespace %s: invalid history_ttl %q", label, text)
		}
		next.HistoryTTLMs = ms
	}
	if next.AllowSubscribeForClient, err = overrideBool(raw, "allow_subscribe_for_client", next.AllowSubscribeForClient, label); err != nil {
		return NamespaceConfig{}, err
	}
	if next.AllowPublishForSubscriber, err = overrideBool(raw, "allow_publish_for_subscriber", next.AllowPublishForSubscriber, label); err != nil {
		return NamespaceConfig{}, err
	}
	if next.AllowPublishForClient, err = overrideBool(raw, "allow_publish_for_client", next.AllowPublishForClient, label); err != nil {
		return NamespaceConfig{}, err
	}
	if next.ForceRecovery, err = overrideBool(raw, "force_recovery", next.ForceRecovery, label); err != nil {
		return NamespaceConfig{}, err
	}
	if next.ForcePositioning, err = overrideBool(raw, "force_positioning", next.ForcePositioning, label); err != nil {
		return NamespaceConfig{}, err
	}
	if next.SubscribeProxyEnabled, err = overrideBool(raw, "subscribe_proxy_enabled", next.SubscribeProxyEnabled, label); err != nil {
		return NamespaceConfig{}, err
	}
	return next, nil
}

func overrideBool(raw map[string]json.RawMessage, key string, current bool, label string) (bool, error) {
	value, ok := raw[key]
	if !ok {
		return current, nil
	}
	parsed, err := parseBool(value, key)
	if err != nil {
		return false, fmt.Errorf("namespace %s: %s must be a boolean", label, key)
	}
	return parsed, nil
}

func overrideInt(raw map[string]json.RawMessage, key string, current int, label string) (int, error) {
	value, ok := raw[key]
	if !ok {
		return current, nil
	}
	parsed, err := parseInt(value, key)
	if err != nil || parsed < 0 {
		return 0, fmt.Errorf("namespace %s: %s must be a non-negative integer", label, key)
	}
	return parsed, nil
}

func object(raw json.RawMessage, name string) (map[string]json.RawMessage, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return nil, fmt.Errorf("%s must be an object", name)
	}
	return fields, nil
}

func parseString(raw json.RawMessage, name string) (string, error) {
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return "", fmt.Errorf("%s must be a string", name)
	}
	return value, nil
}

func parseBool(raw json.RawMessage, name string) (bool, error) {
	var value bool
	if err := json.Unmarshal(raw, &value); err != nil {
		return false, fmt.Errorf("%s must be a boolean", name)
	}
	return value, nil
}

func parseHosts(raw json.RawMessage) ([]string, error) {
	var hosts []string
	if err := json.Unmarshal(raw, &hosts); err != nil {
		return nil, fmt.Errorf("must be an array of host names")
	}
	for _, host := range hosts {
		if host == "" || strings.Contains(host, "://") || strings.Contains(host, "/") {
			return nil, fmt.Errorf("entries must be hosts such as 127.0.0.1:4327")
		}
	}
	return hosts, nil
}

func parseTimeout(raw json.RawMessage, name string) (int64, error) {
	text, err := parseString(raw, name)
	if err != nil {
		return 0, fmt.Errorf("%s must be a duration string", name)
	}
	ms, ok := ParseDuration(text)
	if !ok || ms <= 0 {
		return 0, fmt.Errorf("invalid %s %q", name, text)
	}
	return ms, nil
}

func parseInt(raw json.RawMessage, name string) (int, error) {
	var value int
	if err := json.Unmarshal(raw, &value); err != nil {
		return 0, fmt.Errorf("%s must be an integer", name)
	}
	return value, nil
}

var durationPattern = regexp.MustCompile(`^(\d+)(ms|s|m|h)$`)

// ParseDuration parses values such as 25s, 5m, and 300ms.
func ParseDuration(value string) (int64, bool) {
	match := durationPattern.FindStringSubmatch(strings.TrimSpace(value))
	if match == nil {
		return 0, false
	}
	var amount int64
	for _, ch := range match[1] {
		amount = amount*10 + int64(ch-'0')
	}
	switch match[2] {
	case "ms":
		return amount, true
	case "s":
		return amount * 1000, true
	case "m":
		return amount * 60_000, true
	default:
		return amount * 3_600_000, true
	}
}

var channelName = regexp.MustCompile(`^[A-Za-z0-9_$\-.:#=]{1,255}$`)

// ResolveChannel finds the namespace for a channel name.
func ResolveChannel(cfg Config, channel string) (NamespaceConfig, int, string) {
	if !channelName.MatchString(channel) {
		return NamespaceConfig{}, 107, "bad request"
	}
	colon := strings.IndexByte(channel, ':')
	if colon == -1 {
		for _, ns := range cfg.Namespaces {
			if ns.Name == "" {
				return ns, 0, ""
			}
		}
		return NamespaceConfig{}, 102, "unknown channel"
	}
	if colon == 0 || colon == len(channel)-1 {
		return NamespaceConfig{}, 107, "bad request"
	}
	name := channel[:colon]
	for _, ns := range cfg.Namespaces {
		if ns.Name == name {
			return ns, 0, ""
		}
	}
	return NamespaceConfig{}, 102, "unknown channel"
}

// Glob reports whether value matches a * and ? pattern.
func Glob(pattern, value string) bool {
	if pattern == "" {
		return true
	}
	var b strings.Builder
	b.WriteByte('^')
	for _, r := range pattern {
		switch r {
		case '*':
			b.WriteString(".*")
		case '?':
			b.WriteByte('.')
		default:
			b.WriteString(regexp.QuoteMeta(string(r)))
		}
	}
	b.WriteByte('$')
	re, err := regexp.Compile(b.String())
	if err != nil {
		return false
	}
	return re.MatchString(value)
}
