package realtime

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/coder/websocket"

	"channeld/internal/config"
)

// Running is a listening node.
type Running struct {
	Port  int
	Close func(context.Context) error
}

// Start listens for client and API connections.
func Start(cfg config.Config) (*Running, error) {
	node := New(cfg)
	server := &http.Server{
		Handler:           http.HandlerFunc(node.route),
		ReadHeaderTimeout: 5 * time.Second,
	}
	listener, err := net.Listen("tcp", net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port)))
	if err != nil {
		return nil, err
	}
	go func() { _ = server.Serve(listener) }()
	return &Running{
		Port: listener.Addr().(*net.TCPAddr).Port,
		Close: func(ctx context.Context) error {
			node.Shutdown()
			return server.Shutdown(ctx)
		},
	}, nil
}

func (n *Node) route(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "content-type, x-api-key")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	path := strings.TrimRight(r.URL.Path, "/")
	if path == "" {
		path = "/"
	}
	switch {
	case path == "/connection/websocket":
		n.serveWS(w, r)
	case r.Method == http.MethodGet && path == "/health":
		writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
	case path == "/metrics":
		n.serveMetrics(w, r)
	case r.Method == http.MethodGet && path == "/":
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = io.WriteString(w, "channeld realtime node\nwebsocket  /connection/websocket\nhttp api   /api/publish\nmetrics    /metrics\n")
	case strings.HasPrefix(path, "/api/"):
		n.serveAPI(w, r, strings.TrimPrefix(path, "/api/"))
	default:
		writeJSON(w, http.StatusNotFound, errBody(errMethodNotFound, "method not found"))
	}
}

func (n *Node) serveWS(w http.ResponseWriter, r *http.Request) {
	ws, err := websocket.Accept(w, r, &websocket.AcceptOptions{OriginPatterns: n.cfg.AllowedOrigins})
	if err != nil {
		return
	}
	n.accept(ws, r)
}

func (n *Node) serveAPI(w http.ResponseWriter, r *http.Request, method string) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if !knownAPI(method) {
		writeJSON(w, http.StatusNotFound, errBody(errMethodNotFound, "method not found"))
		return
	}
	if !n.authorized(r) {
		writeJSON(w, http.StatusUnauthorized, errBody(errUnauthorized, "unauthorized"))
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	data, err := io.ReadAll(r.Body)
	if err != nil {
		writeJSON(w, http.StatusRequestEntityTooLarge, errBody(errBadRequest, "bad request"))
		return
	}
	body, err := normalizeBody(data)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errBody(errBadRequest, "bad request"))
		return
	}
	if method == "token" {
		result, apiErr := n.issueToken(body)
		if apiErr != nil {
			writeJSON(w, http.StatusOK, errBody(apiErr.Code, apiErr.Message))
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"result": result})
		return
	}
	result, apiErr := n.dispatch(method, body)
	if apiErr != nil {
		writeJSON(w, http.StatusOK, errBody(apiErr.Code, apiErr.Message))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"result": result})
}

func (n *Node) dispatch(method string, body map[string]json.RawMessage) (map[string]any, *apiError) {
	switch method {
	case "publish":
		return n.publishAPI(body)
	case "broadcast":
		return n.broadcastAPI(body)
	case "presence":
		return n.presenceAPI(body)
	case "presence_stats":
		return n.presenceStatsAPI(body)
	case "history":
		return n.historyAPI(body)
	case "history_remove":
		return n.historyRemoveAPI(body)
	case "channels":
		return n.channelsAPI(body)
	case "info":
		return n.infoAPI(), nil
	case "disconnect":
		return n.disconnectAPI(body)
	case "subscribe":
		return n.subscribeAPI(body)
	case "unsubscribe":
		return n.unsubscribeAPI(body)
	default:
		return nil, &apiError{errMethodNotFound, "method not found"}
	}
}

func knownAPI(method string) bool {
	switch method {
	case "publish", "broadcast", "presence", "presence_stats", "history", "history_remove", "channels", "info", "disconnect", "subscribe", "unsubscribe", "token":
		return true
	default:
		return false
	}
}

func (n *Node) authorized(r *http.Request) bool {
	if n.cfg.Insecure {
		return true
	}
	provided := r.Header.Get("X-API-Key")
	if provided == "" {
		provided = r.URL.Query().Get("api_key")
	}
	return secureEqual(provided, n.cfg.APIKey)
}

func secureEqual(left, right string) bool {
	mac := hmac.New(sha256.New, []byte("channeld"))
	_, _ = mac.Write([]byte(left))
	leftSum := mac.Sum(nil)
	mac.Reset()
	_, _ = mac.Write([]byte(right))
	return hmac.Equal(leftSum, mac.Sum(nil))
}
