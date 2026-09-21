.PHONY: test vet build run

test:
	go test ./...

vet:
	go vet ./...

build:
	go build ./cmd/alertsd

run:
	go run ./cmd/alertsd

