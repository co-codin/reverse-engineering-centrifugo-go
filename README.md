# channeld

A single-node realtime server, written in Go, that speaks the [Centrifugo](https://centrifugal.dev) client protocol and HTTP API.

Centrifugo is a Go pub/sub server: clients open one WebSocket, subscribe to channels, and receive publications. Your application publishes through an HTTP API. That core is small enough to run yourself. channeld is that core, in one process, with an in-memory history. Running many machines, and the hooks Centrifugo uses to call back into an application, can come later.

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
| `internal/token` | HS256 sign and verify, expiry, bad claims, wrong algorithm |
| `internal/engine` | history paging, recovery gaps, expiry, idempotency |
| `internal/realtime` | WebSocket protocol, presence, HTTP API, disconnect, ping timeout |

## Docker

The image listens on 4319 and reads `/configs/channeld.json`.

```bash
make docker
docker run --rm -p 4319:4319 channeld:0.1.0
```

`docker compose up --build` does the same and checks `GET /health`.

## Helm

The chart runs one pod. History is in memory, so `replicaCount` must stay 1. The pod reads its config from a Secret mounted at `/configs/channeld.json` and answers `/health` for readiness.

Build the image and make it visible to the cluster, then:

```bash
helm install channeld charts/channeld \
  --set image.repository=channeld \
  --set image.tag=0.1.0
kubectl port-forward svc/channeld 4319:4319
```

`helm test channeld` requests `/health` from inside the cluster. Turn on `ingress.enabled` when clients should reach the WebSocket from outside the cluster. The API key and HMAC secret in the chart values are the dev defaults; change them before pointing the chart at anything shared.

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

`configs/channeld.json` leaves the HTTP API open (`http_api.insecure`). The dev key, if you turn that off, is `dev-api-key` in the `X-API-Key` header. Connection tokens are HS256 JWTs signed with `dev-secret`. The console mints them at `POST /api/token`, which is a local helper and not part of Centrifugo.

Other HTTP methods: `broadcast`, `presence`, `presence_stats`, `history`, `history_remove`, `channels`, `info`, `disconnect`, `subscribe`, `unsubscribe`.

## Channels

| Name | Who can publish from a client |
| --- | --- |
| no prefix, and `chat:` | a subscriber |
| `news:` | nobody — the HTTP API only |

History keeps the last 100 publications for 5 minutes. Subscriptions are recoverable, so a client that reconnects is sent what it missed. Presence and join/leave are on. `rpc` method `echo` returns its data; other RPC methods answer `104`.

## Left for later

Redis or NATS fan-out, protobuf, SSE, gRPC, subscription proxies, delta compression, and the Centrifugo PRO API. Those are how Centrifugo runs more than one machine and how it calls back into an application. The protocol above is enough for one node and for the official JavaScript client.
