BINARY := udpr

.PHONY: build test lint fmt vet selftest clean

build:
	go build -o $(BINARY) ./cmd/udpr

test:
	go test ./... -race

vet:
	go vet ./...

fmt:
	gofmt -s -w .

lint:
	golangci-lint run

selftest: build
	./$(BINARY) selftest -size 1000000 -loss 0.2

clean:
	rm -f $(BINARY)
