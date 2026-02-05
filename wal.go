package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

/*
WAL (Write-Ahead Log) Format:
Each segment file consists of a sequence of binary encoded entries.

+-------------------+-------------------+-------------------+----------------------+
|   CRC32 (4 bytes) |   Size (8 bytes)  |  Offset (8 bytes) |   Payload (N bytes)  |
+-------------------+-------------------+-------------------+----------------------+
| Checksum of Data  | Length of Payload | Logical Position  | The actual data      |
+-------------------+-------------------+-------------------+----------------------+

- CRC (Cyclic Redundancy Check): Ensures data integrity.
- Size & Offset: Enable fast reading without parsing the entire file.
- Payload: The actual data.
*/

const (
	// Size of various components in the WAL entry header
	crcSize    = 4
	sizeSize   = 8
	offsetSize = 8
	headerSize = crcSize + sizeSize + offsetSize
)

var (
	ErrInvalidCRC = errors.New("wal: invalid crc, data corruption detected")
)

// WAL: Write-Ahead Log structure
type WAL struct {
	mu            sync.RWMutex  // Ensures thread safety (Concurrency)
	dir           string        // Directory to store WAL files
	bufferSize    int           // Size of the write buffer
	segmentSize   int64         // Max size of each segment file
	segmentPrefix string        // Prefix for segment file names
	activeSegment *Segment      // Current active segment for writing
	segments      []*Segment    // List of closed segment files
	syncStrategy  SyncStrategy  // Sync strategy
	stopSync      chan struct{} // Channel to stop background sync goroutine
}

// Segment: Represents a physical file
type Segment struct {
	idx    int           // Number of the segment (0, 1, 2...)
	path   string        // Path to the file
	file   *os.File      // File descriptor
	writer *bufio.Writer // Uses buffer to reduce system calls (High Performance)
	size   int64         // Current size of the file
}

// NewWAL: Initialize and recover data
func New(cfg Config) (*WAL, error) {

	// Ensure WAL directory exists
	if err := os.MkdirAll(cfg.WALDir, 0755); err != nil {
		return nil, err
	}

	// Load default config values if not set
	cfg.SetDefault()

	// Initialize WAL structure
	wal := WAL{
		dir:           cfg.WALDir,
		bufferSize:    cfg.BufferSize,
		segmentSize:   cfg.SegmentSize,
		segmentPrefix: cfg.SegmentPrefix,
		syncStrategy:  cfg.SyncStrategy,
		stopSync:      make(chan struct{}),
	}

	// Load existing segments from disk for recovery
	if err := wal.loadSegments(); err != nil {
		return nil, err
	}

	// Only run background sync if necessary
	// If the SyncStrategyAlways, we skip it to save CPU
	if wal.syncStrategy != SyncStrategyAlways {
		go wal.backgroundSync(time.Duration(cfg.SyncInterval) * time.Millisecond)
	}

	return &wal, nil
}

// loadSegments: Load existing segments from disk during startup
func (w *WAL) loadSegments() error {

	// List all files in the WAL directory
	entries, err := os.ReadDir(w.dir)
	if err != nil {
		return err
	}

	// Filter segment files based on prefix and suffix
	var segFiles []string
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), fmt.Sprintf("%s-", w.segmentPrefix)) && strings.HasSuffix(e.Name(), ".wal") {
			segFiles = append(segFiles, e.Name())
		}
	}

	// Sort to ensure log order
	sort.Strings(segFiles)

	// If no segment files exist, create the first one
	if len(segFiles) == 0 {
		return w.createActiveSegment(0)
	}

	// Reopen old segments
	for i, name := range segFiles {
		path := filepath.Join(w.dir, name)
		f, err := os.OpenFile(path, os.O_RDWR, 0600)
		if err != nil {
			return err
		}

		// Get current size to determine offset
		stat, err := f.Stat()
		if err != nil {
			return err
		}

		// Create Segment struct
		seg := &Segment{
			idx:  i,
			path: path,
			file: f,
			size: stat.Size(),
		}

		// If it's the last file (Active Segment), we need to check for consistency
		if i == len(segFiles)-1 {
			// Seek to the end of the file
			if _, err := f.Seek(0, io.SeekEnd); err != nil {
				return err
			}
			seg.writer = bufio.NewWriterSize(f, w.bufferSize)
			w.activeSegment = seg

			// Important: Repair the last file if corrupted due to crash
			if err := w.repairActiveSegment(); err != nil {
				return fmt.Errorf("corrupted segment repair failed: %v", err)
			}
		} else {
			// Close old segment files
			f.Close()
			w.segments = append(w.segments, seg)
		}
	}
	return nil
}

// repairActiveSegment: Repair the active segment file if corrupted
// Simple logic: Read through the active file to find the last valid point
// In real production, we would do tail reading to optimize. Here we use scanning for clarity.
func (w *WAL) repairActiveSegment() error {

	f := w.activeSegment.file

	// Read from the beginning
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return err
	}

	reader := bufio.NewReader(f)
	var validOffset int64 = 0

	for {
		// Read Header (CRC + Size + Offset)
		header := make([]byte, headerSize)
		if _, err := io.ReadFull(reader, header); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break // End of file or truncated file -> Stop
			}
			return err
		}

		// Parse Header
		readCrc := binary.BigEndian.Uint32(header[:crcSize])
		size := binary.BigEndian.Uint64(header[crcSize : crcSize+sizeSize])
		offset := binary.BigEndian.Uint64(header[crcSize+sizeSize : headerSize])

		// Read Payload
		payload := make([]byte, size)
		if _, err := io.ReadFull(reader, payload); err != nil {
			break // Payload read error -> Stop
		}

		// Verify CRC
		verifyBuf := make([]byte, sizeSize+offsetSize+len(payload))
		binary.BigEndian.PutUint64(verifyBuf[:sizeSize], size)
		binary.BigEndian.PutUint64(verifyBuf[sizeSize:sizeSize+offsetSize], offset)
		copy(verifyBuf[sizeSize+offsetSize:], payload)

		if crc32.ChecksumIEEE(verifyBuf) != readCrc {
			log.Println("⚠️ Detected corrupted data due to crash -> Will truncate.")
			break
		}

		// Valid record, update offset safely
		validOffset += int64(offset)
	}

	// Truncate file to the last valid position
	if err := f.Truncate(validOffset); err != nil {
		return err
	}

	// Update the write state
	if _, err := f.Seek(validOffset, io.SeekStart); err != nil {
		return err
	}
	w.activeSegment.size = validOffset
	w.activeSegment.writer = bufio.NewWriterSize(f, w.bufferSize) // Reset buffer writer

	return nil
}

// createActiveSegment: Create a new active segment
func (w *WAL) createActiveSegment(idx int) error {

	// sync and close old segment if exists
	if w.activeSegment != nil {
		if err := w.sync(); err != nil {
			return err
		}
		w.segments = append(w.segments, w.activeSegment)
	}

	fileName := fmt.Sprintf("%s-%04d.wal", w.segmentPrefix, idx)
	path := filepath.Join(w.dir, fileName)

	// Open file with 0600 permissions (read/write for owner only) -> Security
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0600)
	if err != nil {
		return err
	}

	w.activeSegment = &Segment{
		idx:    idx,
		path:   path,
		file:   f,
		writer: bufio.NewWriterSize(f, w.bufferSize),
		size:   0,
	}
	return nil
}

// backgroundSync: Periodically sync data based on strategy
func (w *WAL) backgroundSync(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Optimize based on Strategy
			switch w.syncStrategy {

			case SyncStrategyBackground:
				if err := w.Sync(); err != nil {
					log.Printf("wal: background sync error: %v", err)
				}

			case SyncStrategyOSCache:
				w.mu.Lock()
				_ = w.activeSegment.file.Sync()
				w.mu.Unlock()
			default:
				// do nothing
			}

		case <-w.stopSync:
			log.Println("wal: stopping sync routine...")
			return
		}
	}
}

// Write: Append a new entry to the WAL
func (w *WAL) Write(payload []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Prepare Header
	// Total size of entry = Header + Payload
	pktSize := int64(headerSize + len(payload))
	// Check Log Rotation
	if w.activeSegment.size+pktSize > w.segmentSize {
		if err := w.createActiveSegment(w.activeSegment.idx + 1); err != nil {
			return err
		}
	}
	// Prepare Buffer
	buf := make([]byte, pktSize)

	// Encoding Binary
	// Format: [CRC][Size][Offset][Payload] -> Calculate CRC over
	binary.BigEndian.PutUint64(buf[crcSize:crcSize+sizeSize], uint64(len(payload)))
	binary.BigEndian.PutUint64(buf[crcSize+sizeSize:headerSize], uint64(w.activeSegment.size))
	copy(buf[headerSize:], payload)

	// Calculate CRC
	checksum := calculateCRC(buf[crcSize:])
	binary.BigEndian.PutUint32(buf[:crcSize], checksum)

	// Write to buffered writer first (Go RAM)
	if _, err := w.activeSegment.writer.Write(buf); err != nil {
		return err
	}

	// Sync based on strategy
	switch w.syncStrategy {

	case SyncStrategyAlways: // safest

		if err := w.sync(); err != nil {
			return err
		}

	case SyncStrategyOSCache: // fast, safe with app crash
		// Push to OS
		// Syncing is handled by the background goroutine every 1 second
		if err := w.activeSegment.writer.Flush(); err != nil {
			return err
		}

	case SyncStrategyBackground: // high risk, super fast
		// do nothing
	}

	// Update Offset for next write
	w.activeSegment.size += pktSize

	return nil
}

// sync: Flush buffer and fsync to disk
// Internal method, caller must hold the lock
func (w *WAL) sync() error {

	if w.activeSegment == nil {
		return nil
	}

	// Flush buffer Go -> OS
	if err := w.activeSegment.writer.Flush(); err != nil {
		return err
	}
	// Fsync OS -> Disk Platter (Mandatory for Durability)
	return w.activeSegment.file.Sync()
}

// Sync: Public method to sync data to disk
// Caller does not need to hold the lock
func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.sync()
}

// StopSync stops sync goroutine
func (w *WAL) StopSync() {
	if w.stopSync != nil {
		close(w.stopSync)
	}
}

// Close: Safely close the WAL
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Ensure all data is written before closing
	if err := w.sync(); err != nil {
		return err
	}

	// Stop background sync goroutine
	w.StopSync()

	// Close active segment file
	return w.activeSegment.file.Close()
}
