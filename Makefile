.PHONY: all build test test-race cover vet fmt lint tidy clean

all: fmt vet test

build:
	go build ./...

test:
	go test ./...

test-race:
	go test -race ./...

cover:
	go test -coverprofile=coverage.txt -covermode=atomic ./...
	go tool cover -html=coverage.txt -o coverage.html

vet:
	go vet ./...

fmt:
	gofmt -l -w .

lint:
	golangci-lint run ./...

tidy:
	go mod tidy

clean:
	rm -f coverage.txt coverage.html
