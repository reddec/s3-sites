.PHONY: build fix lint run

build:
	go build -trimpath -ldflags="-s -w" ./...

fix:
	go fix ./...
	go mod tidy

run:
	go run .

lint:
	golangci-lint run ./...
