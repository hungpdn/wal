.PHONY: all test clean run-example

all: test

test:
	go test -v -race ./...

clean:
	rm -rf wal_data test_wal_data
	go clean

run-example:
	go run examples/main.go

deps:
	go mod tidy