package realtime

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"time"
)

type proxyResponse struct {
	Result     json.RawMessage `json:"result"`
	Error      *proxyError     `json:"error"`
	Disconnect *proxyClose     `json:"disconnect"`
}

type proxyError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type proxyClose struct {
	Code   int    `json:"code"`
	Reason string `json:"reason"`
}

type connectProxyResult struct {
	User     string          `json:"user"`
	ExpireAt int64           `json:"expire_at"`
	Info     json.RawMessage `json:"info"`
	Data     json.RawMessage `json:"data"`
	Channels []string        `json:"channels"`
}

type publishProxyResult struct {
	Data        json.RawMessage `json:"data"`
	SkipHistory bool            `json:"skip_history"`
}

type subscribeProxyResult struct {
	Info     json.RawMessage `json:"info"`
	Data     json.RawMessage `json:"data"`
	Override struct {
		Presence         *boolValue `json:"presence"`
		JoinLeave        *boolValue `json:"join_leave"`
		ForceRecovery    *boolValue `json:"force_recovery"`
		ForcePositioning *boolValue `json:"force_positioning"`
	} `json:"override"`
}

type boolValue struct {
	Value bool `json:"value"`
}

func (n *Node) postProxy(kind, endpoint string, timeout time.Duration, payload any) (proxyResponse, error) {
	start := time.Now()
	resp, err := n.callProxy(endpoint, timeout, payload)
	n.observeProxy(kind, time.Since(start), err)
	return resp, err
}

func (n *Node) callProxy(endpoint string, timeout time.Duration, payload any) (proxyResponse, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return proxyResponse{}, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return proxyResponse{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := n.httpClient.Do(req)
	if err != nil {
		return proxyResponse{}, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return proxyResponse{}, err
	}
	if resp.StatusCode != http.StatusOK {
		return proxyResponse{}, io.ErrUnexpectedEOF
	}
	var decoded proxyResponse
	if err := json.Unmarshal(data, &decoded); err != nil {
		return proxyResponse{}, err
	}
	return decoded, nil
}

func proxyTimeout(ms int64) time.Duration {
	if ms <= 0 {
		return time.Second
	}
	return time.Duration(ms) * time.Millisecond
}
