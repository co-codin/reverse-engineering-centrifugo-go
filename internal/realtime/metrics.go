package realtime

import (
	"fmt"
	"net/http"
	"strconv"
	"time"
)

func (n *Node) observeProxy(kind string, elapsed time.Duration, err error) {
	stat := n.proxies[kind]
	if stat == nil || elapsed < 0 {
		return
	}
	stat.duration.Add(uint64(elapsed))
	if err != nil {
		stat.failed.Add(1)
		return
	}
	stat.ok.Add(1)
}

func (n *Node) sampleCounts() (connections, subscriptions int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	connections = len(n.live)
	for _, members := range n.byChannel {
		subscriptions += len(members)
	}
	return connections, subscriptions
}

func (n *Node) serveMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	connections, subscriptions := n.sampleCounts()
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	fmt.Fprintf(w, "# HELP channeld_connections Current WebSocket connections.\n")
	fmt.Fprintf(w, "# TYPE channeld_connections gauge\n")
	fmt.Fprintf(w, "channeld_connections %d\n", connections)
	fmt.Fprintf(w, "# HELP channeld_subscriptions Current channel subscriptions.\n")
	fmt.Fprintf(w, "# TYPE channeld_subscriptions gauge\n")
	fmt.Fprintf(w, "channeld_subscriptions %d\n", subscriptions)
	fmt.Fprintf(w, "# HELP channeld_publications_total Publications accepted by the node.\n")
	fmt.Fprintf(w, "# TYPE channeld_publications_total counter\n")
	fmt.Fprintf(w, "channeld_publications_total %d\n", n.publications.Load())
	fmt.Fprintf(w, "# HELP channeld_proxy_requests_total Proxy HTTP calls by outcome.\n")
	fmt.Fprintf(w, "# TYPE channeld_proxy_requests_total counter\n")
	fmt.Fprintf(w, "# HELP channeld_proxy_duration_seconds Cumulative proxy HTTP time.\n")
	fmt.Fprintf(w, "# TYPE channeld_proxy_duration_seconds summary\n")
	for _, kind := range []string{"connect", "subscribe", "publish"} {
		stat := n.proxies[kind]
		fmt.Fprintf(w, "channeld_proxy_requests_total{proxy=%q,result=\"ok\"} %d\n", kind, stat.ok.Load())
		fmt.Fprintf(w, "channeld_proxy_requests_total{proxy=%q,result=\"error\"} %d\n", kind, stat.failed.Load())
		seconds := float64(stat.duration.Load()) / float64(time.Second)
		fmt.Fprintf(w, "channeld_proxy_duration_seconds_sum{proxy=%q} %s\n", kind, strconv.FormatFloat(seconds, 'f', 6, 64))
		fmt.Fprintf(w, "channeld_proxy_duration_seconds_count{proxy=%q} %d\n", kind, stat.ok.Load()+stat.failed.Load())
	}
}
