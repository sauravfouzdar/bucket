package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"log"

	"github.com/sauravfouzdar/bucket/pkg/common"
	"github.com/sauravfouzdar/bucket/pkg/master"
)

func main() {
	// Parse command line flags
	addr := flag.String("addr", common.DefaultMasterConfig.Address, "Master server address")
	heartbeatInterval := flag.Int("heartbeat", common.DefaultMasterConfig.HeartbeatInterval, "Heartbeat interval in seconds")
	replicaNum := flag.Int("replica", common.DefaultMasterConfig.ChunkReplicaNum, "Number of chunk replicas")
	leaseTimeout := flag.Int("lease", common.DefaultMasterConfig.LeaseTimeout, "Lease timeout in seconds")
	checkpointInterval := flag.Int("checkpoint", common.DefaultMasterConfig.CheckpointInterval, "Checkpoint interval in seconds")
	flag.Parse()

	// Create master config
	config := common.MasterConfig{
		Address: *addr,
		HeartbeatInterval: *heartbeatInterval,
		ChunkReplicaNum: *replicaNum,
		LeaseTimeout: *leaseTimeout,
		CheckpointInterval: *checkpointInterval,
	}

	// Create and start master
	m := master.NewMaster(config)
	if err := m.Start(); err != nil {
		log.Fatalf("Failed to start master: %v", err)
	}

	// signal for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	<-sigChan

	// Shutdown master
	m.Shutdown()
	fmt.Println("Master stopped")
}
