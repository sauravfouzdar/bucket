```makefile
# GFS Makefile

.PHONY: all build clean test run-master run-chunkserver run-client

# Binary names
MASTER_BIN=gfs-master
CHUNKSERVER_BIN=gfs-chunkserver
CLIENT_BIN=gfs-client

# Build flags
GOFLAGS=-v

# Directories
BUILD_DIR=bin
SRC_DIR=.

all: build

build: build-master build-chunkserver build-client

build-master:
	@echo "Building master server..."
	@mkdir -p $(BUILD_DIR)
	go build $(GOFLAGS) -o $(BUILD_DIR)/$(MASTER_BIN) $(SRC_DIR)/cmd/master

build-chunkserver:
	@echo "Building chunk server..."
	@mkdir -p $(BUILD_DIR)
	go build $(GOFLAGS) -o $(BUILD_DIR)/$(CHUNKSERVER_BIN) $(SRC_DIR)/cmd/chunkserver

build-client:
	@echo "Building client..."
	@mkdir -p $(BUILD_DIR)
	go build $(GOFLAGS) -o $(BUILD_DIR)/$(CLIENT_BIN) $(SRC_DIR)/cmd/client

test:
	@echo "Running tests..."
	go test -v ./...

clean:
	@echo "Cleaning..."
	rm -rf $(BUILD_DIR)

run-master:
	@echo "Running master server..."
	$(BUILD_DIR)/$(MASTER_BIN) $(ARGS)

run-chunkserver:
	@echo "Running chunk server..."
	$(BUILD_DIR)/$(CHUNKSERVER_BIN) $(ARGS)

run-client:
	@echo "Running client..."
	$(BUILD_DIR)/$(CLIENT_BIN) $(ARGS)
```