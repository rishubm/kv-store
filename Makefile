.PHONY: run build test clean

run:
	go run main.go

build:
	go build -o server main.go

test:
	go test -race -v ./...

clean:
	rm -f server
