.PHONY: build test run compose-up compose-down tidy

build:
	go build ./cmd/api

test:
	go test ./...

run:
	go run ./cmd/api

compose-up:
	docker compose up --build

compose-down:
	docker compose down -v

tidy:
	go mod tidy
