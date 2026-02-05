package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
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
|   CRC32 (4 bytes) |   Size (8 bytes)  |   SeqID (8 bytes) |   Payload (N bytes)  |
+-------------------+-------------------+-------------------+----------------------+
| Checksum of Data  | Length of Payload | Monotonic ID      | The actual data      |
+-------------------+-------------------+-------------------+----------------------+

- CRC (Cyclic Redundancy Check): Ensures data integrity.
- Size: Enable fast reading without parsing the entire file.
- SeqID: Global Sequence ID
- Payload: The actual data.
*/

const (
	// Size of various components in the WAL entry header
	crcSize    = 4
	sizeSize   = 8
	seqIDSize  = 8
	headerSize = crcSize + sizeSize + seqIDSize
)

var (
	ErrInvalidCRC = errors.New("wal: invalid crc, data corruption detected")
)

// WAL: Write-Ahead Log structure
type WAL struct {
	mu            sync.RWMutex   // Ensures thread safety
	dir           string         // Directory to store WAL files
	bufferSize    int            // Size of the write buffer
	segmentSize   int64          // Max size of each segment file, soft limit not hard limit
	segmentPrefix string         // Prefix for segment file names
	activeSegment *Segment       // Current active segment for writing
	segments      []*Segment     // List of closed segment files
	syncStrategy  SyncStrategy   // Sync strategy
	stopSync      chan struct{}  // Channel to stop background sync
	wg            sync.WaitGroup // WaitGroup for background sync
	lastSeqID     uint64         // Track the global sequence ID
	bufPool       sync.Pool
}

// Segment: Represents a physical file
type Segment struct {
	idx    uint64        // Number of the segment (0, 1, 2...)
	path   string        // Path to the file
	file   *os.File      // File descriptor (Active only)
	writer *bufio.Writer // Buffered writer (Active only)
	size   int64         // Current size of the file
}

type segmentFile struct {
	name string
	idx  uint64
}

// Open: Initialize and recover data
func Open(dir string, cfg *Config) (*WAL, error) {

	// Ensure WAL directory exists
	if err := os.MkdirAll(dir, PermMkdir); err != nil {
		return nil, err
	}

	// Load default config values if not set
	if cfg == nil {
		cfg = &DefaultConfig
	}
	cfg.SetDefault()

	// Initialize WAL structure
	wal := WAL{
		dir:           dir,
		bufferSize:    cfg.BufferSize,
		segmentSize:   cfg.SegmentSize,
		segmentPrefix: cfg.SegmentPrefix,
		syncStrategy:  cfg.SyncStrategy,
		stopSync:      make(chan struct{}),
		bufPool: sync.Pool{
			New: func() interface{} {
				return make([]byte, 4*KB+headerSize)
			},
		},
	}

	// Load existing segments from disk for recovery
	if err := wal.loadSegments(); err != nil {
		return nil, err
	}

	// Only run background sync if not SyncStrategyAlways
	if wal.syncStrategy != SyncStrategyAlways {
		wal.wg.Add(1)
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

	// Parse the filename and index it into a temporary struct
	var segFiles []segmentFile
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), fmt.Sprintf("%s-", w.segmentPrefix)) &&
			strings.HasSuffix(e.Name(), ".wal") {

			// Parse ID from file name
			var idx uint64
			cleanName := strings.TrimPrefix(e.Name(), w.segmentPrefix+"-")
			cleanName = strings.TrimSuffix(cleanName, ".wal")

			// Scan integer number
			if _, err := fmt.Sscanf(cleanName, "%d", &idx); err == nil {
				segFiles = append(segFiles, segmentFile{name: e.Name(), idx: idx})
			}
		}
	}

	// Sort by Index (Integer Sort)
	sort.Slice(segFiles, func(i, j int) bool {
		return segFiles[i].idx < segFiles[j].idx
	})

	// If no segment files exist, create the first one
	if len(segFiles) == 0 {
		return w.createActiveSegment(0)
	}

	for i, fileObj := range segFiles {
		path := filepath.Join(w.dir, fileObj.name)
		f, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND, PermFileOpen)
		if err != nil {
			return err
		}

		stat, err := f.Stat()
		if err != nil {
			return err
		}

		seg := &Segment{
			idx:  fileObj.idx,
			path: path,
			file: f,
			size: stat.Size(),
		}

		// If it's the last file, set as active
		if i == len(segFiles)-1 {
			// Seek to the end of the file
			if _, err := f.Seek(0, io.SeekEnd); err != nil {
				return err
			}
			seg.writer = bufio.NewWriterSize(f, w.bufferSize)
			w.activeSegment = seg

			// Important: Repair the last file if corrupted due to crash
			if err := w.repairActiveSegment(); err != nil {
				return err
			}
		} else {
			// Close old segment files
			f.Close()
			w.segments = append(w.segments, seg)
		}
	}

	// SPECIAL CASE: If active segment is empty (newly rotated before crash),
	// we need to find lastSeqID from the previous closed segment.
	if w.activeSegment.size == 0 && len(w.segments) > 0 {
		lastClosedSeg := w.segments[len(w.segments)-1]
		lastID, err := w.scanSegmentForLastID(lastClosedSeg.path)
		if err != nil {
			return fmt.Errorf("failed to recover SeqID from previous segment: %v", err)
		}
		w.lastSeqID = lastID
	}
	return nil
}

// scanSegmentForLastID reads a closed segment to find the highest SeqID.
// This is only called once at startup if the active segment is empty.
func (w *WAL) scanSegmentForLastID(path string) (uint64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	reader := bufio.NewReader(f)
	var lastID uint64

	for {
		header := make([]byte, headerSize)
		if _, err := io.ReadFull(reader, header); err != nil {
			if err == io.EOF {
				break
			}
			return 0, err
		}
		size := binary.BigEndian.Uint64(header[crcSize : crcSize+sizeSize])
		seqID := binary.BigEndian.Uint64(header[crcSize+sizeSize : headerSize])
		lastID = seqID

		// Skip payload
		if _, err := reader.Discard(int(size)); err != nil {
			return 0, err
		}
	}
	return lastID, nil
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
	var maxSeqID uint64 = 0

	for {
		// Read Header (CRC + Size + Offset)
		header := make([]byte, headerSize)
		if _, err := io.ReadFull(reader, header); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return err
		}

		// Parse Header
		readCrc := binary.BigEndian.Uint32(header[:crcSize])
		size := binary.BigEndian.Uint64(header[crcSize : crcSize+sizeSize])
		seqID := binary.BigEndian.Uint64(header[crcSize+sizeSize : headerSize])

		// Read Payload
		payload := make([]byte, size)
		if _, err := io.ReadFull(reader, payload); err != nil {
			break
		}

		// Verify CRC
		verifyBuf := make([]byte, sizeSize+seqIDSize+len(payload))
		binary.BigEndian.PutUint64(verifyBuf[:sizeSize], size)
		binary.BigEndian.PutUint64(verifyBuf[sizeSize:sizeSize+seqIDSize], seqID)
		copy(verifyBuf[sizeSize+seqIDSize:], payload)

		if calculateCRC(verifyBuf) != readCrc {
			log.Println("⚠️ WAL: Corrupted tail detected, truncating...")
			break
		}

		validOffset += int64(headerSize + size)
		maxSeqID = seqID
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
	w.lastSeqID = maxSeqID                                        // Update global state

	return nil
}

// createActiveSegment: Create a new active segment
func (w *WAL) createActiveSegment(idx uint64) error {

	// sync and close old segment if exists
	if w.activeSegment != nil {
		// Flush & Sync last time
		if err := w.sync(); err != nil {
			return err
		}
		// Close file descriptor file to free up resources.
		if err := w.activeSegment.file.Close(); err != nil {
			return err
		}
		// Put into storage list (only path and size are needed for Iterator later)
		w.segments = append(w.segments, w.activeSegment)
	}

	fileName := fmt.Sprintf("%s-%04d.wal", w.segmentPrefix, idx)
	path := filepath.Join(w.dir, fileName)

	// Open file with 0600 permissions (read/write for owner only) -> Security
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, PermFileOpen)
	if err != nil {
		return err
	}

	// Sync parent directory
	// The file has been synced, but its entry in the parent directory may not have been synced to disk
	// If the power goes out at the moment the new file is created, that file may disappear completely
	if dir, err := os.Open(w.dir); err == nil {
		dir.Sync() // Ensure that the entry for the new file is written to the directory table
		dir.Close()
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
	defer w.wg.Done()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			switch w.syncStrategy {

			case SyncStrategyBackground:

				if err := w.Sync(); err != nil {
					log.Printf("wal: background sync error: %v", err)
				}

			case SyncStrategyOSCache:
				// Minimized Lock Contention
				w.mu.Lock()
				if w.activeSegment == nil || w.activeSegment.file == nil {
					w.mu.Unlock()
					continue
				}
				// Copy file pointer to local variable
				f := w.activeSegment.file
				w.mu.Unlock()

				err := f.Sync()
				if err != nil && !errors.Is(err, os.ErrClosed) {
					log.Printf("wal: background sync error: %v", err)
				}

			default:
				// do nothing
			}

		case <-w.stopSync:
			return
		}
	}
}

func (w *WAL) bufferWrite(payload []byte) error {
	w.lastSeqID++

	pktSize := int64(headerSize + len(payload))
	payloadLen := uint64(len(payload))

	currentSeqID := w.lastSeqID

	var buf []byte
	obj := w.bufPool.Get()
	if b, ok := obj.([]byte); ok && int64(cap(b)) >= pktSize {
		buf = b[:pktSize]
	} else {
		buf = make([]byte, pktSize)
	}

	// [CRC][Size][SeqID][Payload]
	binary.BigEndian.PutUint64(buf[crcSize:crcSize+sizeSize], payloadLen)
	binary.BigEndian.PutUint64(buf[crcSize+sizeSize:headerSize], currentSeqID)
	copy(buf[headerSize:], payload)

	checksum := calculateCRC(buf[crcSize:])
	binary.BigEndian.PutUint32(buf[:crcSize], checksum)

	if _, err := w.activeSegment.writer.Write(buf); err != nil {
		return err
	}

	w.bufPool.Put(buf)
	w.activeSegment.size += pktSize
	return nil
}

func (w *WAL) runSyncStrategy() error {
	switch w.syncStrategy {
	case SyncStrategyAlways:
		return w.sync()
	case SyncStrategyOSCache:
		return w.activeSegment.writer.Flush()
	}
	return nil
}

// Write: Append a new entry to the WAL
func (w *WAL) Write(payload []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	pktSize := int64(headerSize + len(payload))
	if w.activeSegment.size+pktSize > w.segmentSize {
		if err := w.createActiveSegment(w.activeSegment.idx + 1); err != nil {
			return err
		}
	}

	if err := w.bufferWrite(payload); err != nil {
		return err
	}

	return w.runSyncStrategy()
}

// WriteBatch writes a batch of entries to the WAL efficiently.
// It acquires the lock once and syncs once (if needed) for the whole batch.
func (w *WAL) WriteBatch(payloads [][]byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	var batchSize int64
	for _, p := range payloads {
		batchSize += int64(headerSize + len(p))
	}

	if w.activeSegment.size+batchSize > w.segmentSize {
		if err := w.createActiveSegment(w.activeSegment.idx + 1); err != nil {
			return err
		}
	}

	for _, payload := range payloads {
		if err := w.bufferWrite(payload); err != nil {
			return err
		}
	}

	return w.runSyncStrategy()
}

// sync: Flush buffer and fsync to disk
// Internal method, caller must hold the lock
func (w *WAL) sync() error {
	if w.activeSegment == nil {
		return nil
	}
	if err := w.activeSegment.writer.Flush(); err != nil {
		return err
	}
	return w.activeSegment.file.Sync()
}

// Sync: Public method to sync data to disk
// Caller does not need to hold the lock
func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.sync()
}

// StopSync stops sync goroutine and WAITS for it to finish
func (w *WAL) StopSync() {
	select {
	case <-w.stopSync:
	default:
		if w.stopSync != nil {
			close(w.stopSync)
		}
	}
	w.wg.Wait()
}

// Close: Safely close the WAL
func (w *WAL) Close() error {
	w.mu.Lock()
	if err := w.sync(); err != nil {
		w.mu.Unlock()
		return err
	}
	w.mu.Unlock() // <--- IMPORTANT: Release the lock so that BackgroundSync can finish running (if it's stuck).

	w.StopSync()

	w.mu.Lock()
	defer w.mu.Unlock()
	return w.activeSegment.file.Close()
}

func (w *WAL) GetLastSegmentIdx() uint64 {
	w.mu.RLock()
	defer w.mu.RUnlock()

	if w.activeSegment == nil {
		return 0
	}

	return w.activeSegment.idx
}

// TruncateFront deletes all closed segments with an index less than the provided index.
// This is used to free up disk space after a snapshot has been taken.
// Note: This does not affect the active segment.
func (w *WAL) TruncateFront(segmentIdx uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	var keptSegments []*Segment
	for _, seg := range w.segments {
		if seg.idx < segmentIdx {
			// Remove the file from disk
			if err := os.Remove(seg.path); err != nil {
				// If the file is already gone, ignore the error
				if !os.IsNotExist(err) {
					return err
				}
			}
		} else {
			keptSegments = append(keptSegments, seg)
		}
	}

	// Update the segment list
	w.segments = keptSegments
	return nil
}

// Cleanup removes closed segments that are older than the specified TTL (Time To Live).
// This is useful for time-based retention policies.
func (w *WAL) CleanupByTTL(ttl time.Duration) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	threshold := time.Now().Add(-ttl)
	var keptSegments []*Segment

	for _, seg := range w.segments {
		// Get file stats to check modification time
		info, err := os.Stat(seg.path)
		if err != nil {
			// If file doesn't exist, skip it (it's essentially cleaned)
			if os.IsNotExist(err) {
				continue
			}
			// If we can't stat it for some other reason, keep it to be safe
			keptSegments = append(keptSegments, seg)
			continue
		}

		// If the file is older than the threshold, delete it
		if info.ModTime().Before(threshold) {
			if err := os.Remove(seg.path); err != nil {
				if !os.IsNotExist(err) {
					return err
				}
			}
		} else {
			keptSegments = append(keptSegments, seg)
		}
	}

	w.segments = keptSegments
	return nil
}

// CleanupBySize removes old segments if the total WAL size exceeds maxSizeBytes.
// It deletes from the oldest segment until the total size is within the limit.
func (w *WAL) CleanupBySize(maxSizeBytes int64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	var totalSize int64
	for _, seg := range w.segments {
		totalSize += seg.size
	}
	if w.activeSegment != nil {
		totalSize += w.activeSegment.size
	}

	if totalSize <= maxSizeBytes {
		return nil
	}

	var deleteCount int
	for _, seg := range w.segments {
		if totalSize > maxSizeBytes {
			if err := os.Remove(seg.path); err != nil && !os.IsNotExist(err) {
				return err
			}
			totalSize -= seg.size
			deleteCount++
		} else {
			break
		}
	}

	if deleteCount > 0 {
		w.segments = w.segments[deleteCount:]
	}
	return nil
}
