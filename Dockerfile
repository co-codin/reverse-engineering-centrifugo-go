FROM golang:1.22.12-bookworm AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/channeld ./cmd/channeld

FROM alpine:3.21

RUN apk add --no-cache ca-certificates wget \
	&& adduser -D -H -u 65532 nonroot
COPY --from=build /out/channeld /channeld
COPY configs/channeld.json /configs/channeld.json
USER nonroot
ENV CHANNEL_CONFIG=/configs/channeld.json
EXPOSE 4319
ENTRYPOINT ["/channeld"]
