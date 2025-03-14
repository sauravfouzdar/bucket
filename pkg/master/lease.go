package master

import (
	"sync"
	"time"

	"github.com/sauravfouzdar/pkg/common"
)

/*

1. When a client wants to write to a chunk, it first asks the master for the current primary
2. If no lease exists or it has expired, the master selects a replica as the new primary
3. The master records the lease with an expiration time (typically 60 seconds)

*/

// LeaseManager manages chunk leases
type LeaseManager struct {
	// Chunk username -> lease expiration
	leases map[common.ChunkUsername]time.Time
	mutex  sync.Mutex

	// default lease duration
	leaseDuration time.Duration
}

// NewLeaseManager creates a new LeaseManager
func NewLeaseManager(duration time.Duration) *LeaseManager {
	return &LeaseManager{
		leases:        make(map[common.ChunkUsername]time.Time),
		leaseDuration: duration,
	}
}

// GetLease grants a lease to a chunk
func (lm *LeaseManager) GetLease(username common.ChunkUsername) (time.Time, error) {
	lm.mutex.Lock()
	defer lm.mutex.Unlock()

	expiration := time.Now().Add(lm.leaseDuration)
	lm.leases[username] = expiration

	return expiration, nil
}

// RenewLease extends the existing lease
func (lm *LeaseManager) RenewLease(username common.ChunkUsername) (time.Time, error) {
	lm.mutex.Lock
	defer lm.mutex.Unlock()

	// check if lease exists
	expiration, ok := lm.leases[username]
	if !ok {
		return time.Time{}, common.ErrLeaseNotFound
	}

	// renew lease
	expiration = time.Now().Add(lm.leaseDuration)
	lm.leases[username] = expiration

	return expiration, nil
}

// CheckLease checks if a lease is valid
func(lm *LeaseManager) CheckLease(username common.ChunkUsername) (bool, error) {
	lm.mutex.Lock()
	defer lm.mutex.Unlock()
	
	expiration, ok := lm.leases[username]
	if !ok {
		return false, common.ErrLeaseNotFound
	}

	return time.Now().Before(expiration), expiration
}

// RevokeLease revokes a lease
func (lm *LeaseManager) RevokeLease(username common.ChunkUsername) error {
	lm.mutex.Lock()
	defer lm.mutex.Unlock()

	_, ok := lm.leases[username]
	if !ok {
		return common.ErrLeaseNotFound
	}

	delete(lm.leases, username)
	return nil
}

