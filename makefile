# Clean build artifacts
clean:
	rm -rf metadata/

# Run a single node
run-master:
	go run ./cmd/master/main.go --addr localhost:8000

# Run a local cluster of 3 nodes
run-chunkserver-cluster:
	mkdir -p /tmp/chunks1 /tmp/chunks2 /tmp/chunks3
	go run ./cmd/chunkserver/main.go --addr localhost:8001 --root /tmp/chunks1 & \
	go run ./cmd/chunkserver/main.go --addr localhost:8002 --root /tmp/chunks2 & \
	go run ./cmd/chunkserver/main.go --addr localhost:8003 --root /tmp/chunks3

run-client:
	go run ./cmd/client/client.go --master localhost:8000

