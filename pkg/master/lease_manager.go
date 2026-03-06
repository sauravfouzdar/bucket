package master

import (
	"sync"
	"time"

	"github.com/sauravfouzdar/bucket/pkg/common"
)

// LeaseInfo holds complete lease state for a chunk
type LeaseInfo struct {
	Primary     string   // address of the primary chunkserver
	Secondaries []string // addresses of secondary replicas
	Expiration  time.Time
}

// LeaseManager manages primary leases for chunks
type LeaseManager struct {
	leases        map[common.ChunkUsername]*LeaseInfo
	mutex         sync.Mutex
	leaseDuration time.Duration
}

// NewLeaseManager creates a new LeaseManager
func NewLeaseManager(duration time.Duration) *LeaseManager {
	return &LeaseManager{
		leases:        make(map[common.ChunkUsername]*LeaseInfo),
		leaseDuration: duration,
	}
}

// GrantLease grants a primary lease to a specific server for a chunk
func (lm *LeaseManager) GrantLease(username common.ChunkUsername, primary string, secondaries []string) (time.Time, error) {
	lm.mutex.Lock()
	defer lm.mutex.Unlock()

	expiration := time.Now().Add(lm.leaseDuration)
	lm.leases[username] = &LeaseInfo{
		Primary:     primary,
		Secondaries: secondaries,
		Expiration:  expiration,
	}
	return expiration, nil
}

// GetLease returns the current lease info for a chunk, or nil if expired/missing
func (lm *LeaseManager) GetLease(username common.ChunkUsername) *LeaseInfo {
	lm.mutex.Lock()
	defer lm.mutex.Unlock()

	info, ok := lm.leases[username]
	if !ok || time.Now().After(info.Expiration) {
		return nil
	}
	return info
}

// RenewLease extends an existing lease
func (lm *LeaseManager) RenewLease(username common.ChunkUsername) (time.Time, error) {
	lm.mutex.Lock()
	defer lm.mutex.Unlock()

	info, ok := lm.leases[username]
	if !ok {
		return time.Time{}, common.ErrLeaseNotFound
	}

	expiration := time.Now().Add(lm.leaseDuration)
	info.Expiration = expiration
	return expiration, nil
}

// CheckLease reports whether a valid lease exists and returns its info
func (lm *LeaseManager) CheckLease(username common.ChunkUsername) (bool, *LeaseInfo) {
	lm.mutex.Lock()
	defer lm.mutex.Unlock()

	info, ok := lm.leases[username]
	if !ok {
		return false, nil
	}
	return time.Now().Before(info.Expiration), info
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

	info, ok := lm.leases[username]
	if !ok {
		return true
	}
	return time.Now().After(info.Expiration)
}

// CleanExpired removes all expired leases
func (lm *LeaseManager) CleanExpired() {
	lm.mutex.Lock()
	defer lm.mutex.Unlock()

	now := time.Now()
	for username, info := range lm.leases {
		if now.After(info.Expiration) {
			delete(lm.leases, username)
		}
	}
}
