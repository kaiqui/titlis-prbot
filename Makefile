.PHONY: run build lint test tidy

run:
	TITLIS_API_INTERNAL_SECRET=dev-secret \
	PRBOT_USE_MEMORY_PROVIDER=true \
	PRBOT_DISABLE_TEMPORAL=true \
	go run ./cmd/prbot

build:
	CGO_ENABLED=0 go build -ldflags="-s -w" -o bin/prbot ./cmd/prbot

lint:
	go vet ./...

test:
	go test ./... -count=1

tidy:
	go mod tidy
