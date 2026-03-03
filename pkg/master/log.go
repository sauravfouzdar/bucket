package master

// NewOperationLog creates a new operation log at path
func NewOperationLog(path string) (*OperationLog, error) {
	// TODO: implement
	return &OperationLog{}, nil
}

// AppendToLog appends an operation to the log
func (ol *OperationLog) AppendToLog(entry LogEntry) error {
	ol.entries = append(ol.entries, entry)
	return nil
}

// Checkpoint creates a checkpoint of the current master state
func (ol *OperationLog) Checkpoint(master *Master) error {
	// TODO: implement
	return nil
}

// RecoverFromLog replays log entries to rebuild master state
func (ol *OperationLog) RecoverFromLog(master *Master) error {
	// TODO: implement
	return nil
}

// Close closes the operation log file
func (ol *OperationLog) Close() error {
	if ol.logFile != nil {
		return ol.logFile.Close()
	}
	return nil
}
