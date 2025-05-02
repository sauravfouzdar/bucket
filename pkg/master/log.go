package master

// NewOperationLog creates a new operation log
func NewOperationLog(path string) (*OperationLog, error)

// AppendToLog appends an operation to the log
func (ol *OperationLog) AppendToLog(entry LogEntry) error

// Checkpoint creates a checkpoint of the current state
func (ol *OperationLog) Checkpoint(master *Master) error

// RecoverFromLog recovers the master state from the log
func (ol *OperationLog) RecoverFromLog(master *Master) error

// Close closes the operation log
func (ol *OperationLog) Close() error