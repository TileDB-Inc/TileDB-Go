set -e -x
CGO_ENABLED=1 CGO_LDFLAGS='-fsanitize=address' CGO_CFLAGS='-fsanitize=address' go build cmd/tiledb-go/main.go
./main
