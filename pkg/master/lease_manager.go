package master

import (
	"sync"
	"time"

	"github.com/sauravfouzdar/bucket/pkg/common"
)

// LeaseManager manages primary leases for chunks
type LeaseManager struct {
	leases        map[common.ChunkUsername]time.Time
	mutex         sync.Mutex
	leaseDuration time.Duration
}

// NewLeaseManager creates a new LeaseManager
func NewLeaseManager(duration time.Duration) *LeaseManager {
	return &LeaseManager{
		leases:        make(map[common.ChunkUsername]time.Time),
		leaseDuration: duration,
	}
}

// GrantLease grants a lease to a chunk and returns the expiration time
func (lm *LeaseManager) GrantLease(username common.ChunkUsername) (time.Time, error) {
	lm.mutex.Lock()
	defer lm.mutex.Unlock()

	expiration := time.Now().Add(lm.leaseDuration)
	lm.leases[username] = expiration
	return expiration, nil
}

// RenewLease extends an existing lease
func (lm *LeaseManager) RenewLease(username common.ChunkUsername) (time.Time, error) {
	lm.mutex.Lock()
	defer lm.mutex.Unlock()

	if _, ok := lm.leases[username]; !ok {
		return time.Time{}, common.ErrLeaseNotFound
	}

	expiration := time.Now().Add(lm.leaseDuration)
	lm.leases[username] = expiration
	return expiration, nil
}

// CheckLease reports whether a lease is still valid and its expiration
func (lm *LeaseManager) CheckLease(username common.ChunkUsername) (bool, time.Time) {
	lm.mutex.Lock()
	defer lm.mutex.Unlock()

	expiration, ok := lm.leases[username]
	if !ok {
		return false, time.Time{}
	}
	return time.Now().Before(expiration), expiration
}

// RevokeLease removes a lease
func (lm *LeaseManager) RevokeLease(username common.ChunkUsername) error {
	lm.mutex.Lock()
	defer lm.mutex.Unlock()

	if _, ok := lm.leases[username]; !ok {
		return common.ErrLeaseNotFound
	}
	delete(lm.leases, username)
	return nil
}

// LeaseExpired reports whether a lease has expired (or does not exist)
func (lm *LeaseManager) LeaseExpired(username common.ChunkUsername) bool {
	lm.mutex.Lock()
	defer lm.mutex.Unlock()

	expiration, ok := lm.leases[username]
	if !ok {
		return true
	}
	return time.Now().After(expiration)
}

// CleanExpired removes all expired leases
func (lm *LeaseManager) CleanExpired() {
	lm.mutex.Lock()
	defer lm.mutex.Unlock()

	now := time.Now()
	for username, expiration := range lm.leases {
		if now.After(expiration) {
			delete(lm.leases, username)
		}
	}
}
