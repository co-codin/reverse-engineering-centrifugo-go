.PHONY: test dev server docker

test:
	go test ./...

dev:
	bash scripts/dev.sh

server:
	go run ./cmd/channeld

docker:
	docker build -t channeld:0.1.0 .
