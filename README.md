# channeld

A single-node realtime server, written in Go, that speaks the [Centrifugo](https://centrifugal.dev) client protocol and HTTP API.

Centrifugo is a Go pub/sub server: clients open one WebSocket, subscribe to channels, and receive publications. Your application publishes through an HTTP API. That core is small enough to run yourself. channeld is that core, in one process, with an in-memory history. Connect, subscribe, and publish can each ask your application before they proceed. Running many machines can come later.

## Layout

```
cmd/channeld/          process entrypoint
internal/              config, tokens, history, protocol, HTTP API
configs/channeld.json  shipped node config
web/                   browser console
charts/channeld/       Helm chart
Dockerfile             container image
```

`web/` is only the console. It is a Vite app with two `centrifuge-js` clients. It is not the server, so its `package.json` stays in that folder.

## Run

Go 1.22 or newer, and Node for the console.

```bash
cd web && npm install
make dev
```

- Console: http://127.0.0.1:4327
- Node: http://127.0.0.1:4319

`make server` or `go run ./cmd/channeld` runs the node without the console. It reads `configs/channeld.json`, or the file in `CHANNEL_CONFIG`.

## Tests

```bash
make test
```

That runs `go test ./...`:

| Package | What it checks |
| --- | --- |
| `internal/config` | durations, namespace inheritance, channel names, glob matching |
| `internal/token` | HS256 connection and subscription tokens, expiry, bad claims, wrong algorithm |
| `internal/engine` | history paging, recovery gaps, expiry, idempotency |
| `internal/realtime` | WebSocket protocol, origin check, connect, subscribe, and publish proxies, channel tokens, refresh, metrics, presence, HTTP API, shutdown, ping timeout |

## Docker

The image listens on 4319 and reads `/configs/channeld.json`.

```bash
make docker
docker run --rm -p 4319:4319 channeld:0.1.0
```

`docker compose up --build` does the same and checks `GET /health`.

## Helm

The chart runs one pod. History is in memory, so `replicaCount` must stay 1. The pod reads its config from a Secret mounted at `/configs/channeld.json` and answers `/health` for readiness. On SIGTERM it sends disconnect code 3001, which tells clients to reconnect, and waits up to 5 seconds. `GET /metrics` exposes connections, subscriptions, publications, and proxy latency. A push to `main` publishes `ghcr.io/co-codin/reverse-engineering-centrifugo-go:0.1.0` after `go test ./...` passes.

The chart refuses the dev API key, the dev HMAC secret, an open HTTP API, and anonymous clients. Set those before installing:

```bash
helm install channeld charts/channeld \
  --set config.httpApi.key="$API_KEY" \
  --set config.client.tokenHmacSecret="$TOKEN_SECRET" \
  --set config.client.allowedOrigins[0]=app.example
kubectl port-forward svc/channeld 4319:4319
```

`allowedOrigins` lists browser hosts that may open the WebSocket from another origin, such as `app.example` or `127.0.0.1:4327`. An empty list allows only the same host. Clients with no `Origin` header, including the Go tests, are still accepted.

`helm test channeld` requests `/health` from inside the cluster. Turn on `ingress.enabled` when clients should reach the WebSocket from outside the cluster.

`docker run` and `make dev` use `configs/channeld.json`, which leaves the HTTP API open and allows anonymous clients so the console can run on one machine. That file is not what the chart installs.

## Talk to it

WebSocket: `ws://127.0.0.1:4319/connection/websocket`

A client sends newline-delimited JSON commands and the server answers with replies. Pushes (publications, join, leave) have no `id`. An empty `{}` from the server is a ping; the client answers with `{}`.

```json
{"id":1,"connect":{"name":"js"}}
{"id":2,"subscribe":{"channel":"chat:lobby"}}
```

Publish from your backend:

```bash
curl -X POST http://127.0.0.1:4319/api/publish \
  -H 'content-type: application/json' \
  -d '{"channel":"chat:lobby","data":{"text":"hello"}}'
```

`configs/channeld.json` leaves the HTTP API open (`http_api.insecure`). The dev key, if you turn that off, is `dev-api-key` in the `X-API-Key` header. Connection tokens are HS256 JWTs signed with `dev-secret`. `exp` in that token is the session deadline. The connect reply then includes `expires` and `ttl`, and a later `refresh` command with a new token extends it. A subscription token has `sub`, `channel`, and `exp`. Present it on `subscribe` and the node allows that channel without calling the subscribe proxy. `sub_refresh` extends it the same way. The console mints connection tokens at `POST /api/token`. Pass `channel` instead of `channels` to mint a subscription token. That route is a local helper and not part of Centrifugo.

Other HTTP methods: `broadcast`, `presence`, `presence_stats`, `history`, `history_remove`, `channels`, `info`, `disconnect`, `subscribe`, `unsubscribe`.

## Channels

| Name | Who can publish from a client |
| --- | --- |
| no prefix, and `chat:` | a subscriber |
| `news:` | nobody — the HTTP API only |

History keeps the last 100 publications for 5 minutes. Subscriptions are recoverable, so a client that reconnects is sent what it missed. Presence and join/leave are on. `rpc` method `echo` returns its data; other RPC methods answer `104`.

## Application proxies

A connect proxy runs when a browser connects without a JWT. A subscribe proxy runs for namespaces that set `subscribe_proxy_enabled`, unless the subscribe command carries a valid channel token. A publish proxy runs for namespaces that set `publish_proxy_enabled`, after the namespace has allowed the client to publish. All three are HTTP POSTs with the Centrifugo proxy JSON. Leave them unset and the node keeps using the token and the namespace flags. An unreachable publish or subscribe proxy returns error 100 and leaves the connection open.

```json
{
  "client": {
    "proxy": {
      "connect": {
        "enabled": true,
        "endpoint": "http://app:3000/centrifugo/connect",
        "timeout": "1s"
      }
    }
  },
  "channel": {
    "proxy": {
      "subscribe": {
        "endpoint": "http://app:3000/centrifugo/subscribe",
        "timeout": "1s"
      },
      "publish": {
        "endpoint": "http://app:3000/centrifugo/publish",
        "timeout": "1s"
      }
    },
    "namespaces": [
      {
        "name": "chat",
        "subscribe_proxy_enabled": true,
        "publish_proxy_enabled": true
      }
    ]
  }
}
```

The connect handler returns `{"result":{"user":"56"}}`. The subscribe handler returns `{"result":{}}` to allow the channel, or `{"error":{"code":403,"message":"permission denied"}}` to refuse it. The publish handler returns `{"result":{"data":{"text":"hello"}}}` to replace the publication, or an error object to refuse it. `Cookie` and `Authorization` from the WebSocket handshake are forwarded on the connect request as `headers`.

## Left for later

Redis or NATS fan-out, protobuf, SSE, gRPC, delta compression, and the Centrifugo PRO API. Those are how Centrifugo runs more than one machine. The protocol above is enough for one node and for the official JavaScript client.
