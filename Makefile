VERSION ?= 1.0.0
COMMIT  ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)

.PHONY: build test test-verbose lint clean install coverage

build:
	go build -ldflags="-X main.Version=$(VERSION) -X main.Commit=$(COMMIT)" \
	  -o bin/daemonseed ./cmd/daemonseed

test:
	go test -race -count=1 -timeout=120s ./...

test-verbose:
	go test -race -count=1 -timeout=120s -v ./...

lint:
	golangci-lint run ./...

clean:
	rm -rf bin/ coverage.out coverage.html

install:
	go install -ldflags="-X main.Version=$(VERSION) -X main.Commit=$(COMMIT)" ./cmd/daemonseed

coverage:
	go test -race -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html
