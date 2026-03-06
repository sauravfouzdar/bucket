package master

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/sauravfouzdar/bucket/pkg/common"
)

// NewOperationLog opens or creates the WAL file at path.
func NewOperationLog(path string) (*OperationLog, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return nil, fmt.Errorf("create log directory: %w", err)
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("open log file: %w", err)
	}
	return &OperationLog{logFile: f}, nil
}

// AppendToLog writes entry to the WAL and fsyncs for durability.
func (ol *OperationLog) AppendToLog(entry LogEntry) error {
	entry.Timestamp = time.Now()
	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("marshal log entry: %w", err)
	}
	data = append(data, '\n')
	if _, err := ol.logFile.Write(data); err != nil {
		return fmt.Errorf("write log entry: %w", err)
	}
	return ol.logFile.Sync()
}

// Checkpoint flushes metadata to disk and truncates the WAL.
// After a successful checkpoint the log file is empty — recovery
// will load directly from the checkpoint JSON files.
func (ol *OperationLog) Checkpoint(master *Master) error {
	if err := master.metadata.SaveToDisk(); err != nil {
		return fmt.Errorf("checkpoint SaveToDisk: %w", err)
	}
	if err := ol.logFile.Truncate(0); err != nil {
		return fmt.Errorf("truncate log: %w", err)
	}
	if _, err := ol.logFile.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seek log: %w", err)
	}
	log.Printf("WAL checkpoint complete, log truncated")
	return nil
}

// RecoverFromLog replays all entries in the WAL on top of the
// already-loaded checkpoint state.
func (ol *OperationLog) RecoverFromLog(master *Master) error {
	if _, err := ol.logFile.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seek log for recovery: %w", err)
	}

	replayed := 0
	scanner := bufio.NewScanner(ol.logFile)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var entry LogEntry
		if err := json.Unmarshal(line, &entry); err != nil {
			return fmt.Errorf("corrupt WAL entry: %w", err)
		}
		if err := applyLogEntry(master, entry); err != nil {
			log.Printf("WAL replay warning (op=%s): %v", entry.Operation, err)
		}
		replayed++
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("reading WAL: %w", err)
	}

	// Restore file position to end so subsequent appends work correctly.
	if _, err := ol.logFile.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("seek log to end: %w", err)
	}

	if replayed > 0 {
		log.Printf("WAL recovery: replayed %d entries", replayed)
	}
	return nil
}

// Close closes the operation log file.
func (ol *OperationLog) Close() error {
	if ol.logFile != nil {
		return ol.logFile.Close()
	}
	return nil
}

// applyLogEntry dispatches a single WAL entry to the appropriate metadata method.
func applyLogEntry(master *Master, entry LogEntry) error {
	switch entry.Operation {
	case "CREATE_FILE":
		return master.metadata.applyCreateFile(entry.Path, entry.FileID)
	case "DELETE_FILE":
		err := master.metadata.DeleteFile(entry.Path)
		if err == common.ErrFileNotFound {
			return nil // already deleted — idempotent
		}
		return err
	case "CREATE_CHUNK":
		return master.metadata.applyCreateChunk(entry.FileID, entry.ChunkIndex, entry.ChunkHandle)
	default:
		log.Printf("WAL: unknown operation %q, skipping", entry.Operation)
		return nil
	}
}
