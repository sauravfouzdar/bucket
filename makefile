.PHONY: build test clean run-master run-cluster

# Build the raft node binary
# build:
# 	go build -o bin/kv-store ./examples/simple_kv
#go build -o bin/raftnode ./cmd/raftnode


# Run tests
# test:
# 	go test -v ./...

# Clean build artifacts
clean:
	rm -rf bin/
	rm -rf raft-data/

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

# # Run client
# run-client:
# 	mkdir -p raft-data
# 	./bin/kv-store --id node1 --addr localhost:8001 --http localhost:8081 \
#                --peers node2=localhost:8002,node3=localhost:8003 --log-level info & \
# 	./bin/kv-store --id node2 --addr localhost:8002 --http localhost:8082 \
# 			   --peers node1=localhost:8001,node3=localhost:8003 --log-level info & \
# 	./bin/kv-store --id node3 --addr localhost:8003 --http localhost:8083 \
# 			   --peers node1=localhost:8001,node2=localhost:8002 --log-level info
