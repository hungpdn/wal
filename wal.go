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

+-------------------+-------------------+-------------------+----------------------+-------------------+
|   CRC32 (4 bytes) |   Size (8 bytes)  |   SeqID (8 bytes) |   Payload (N bytes)  |   Size (8 bytes)  |
+-------------------+-------------------+-------------------+----------------------+-------------------+
| Checksum of Data  | Length of Payload | Monotonic ID      | The actual data      | Backward Pointer  |
+-------------------+-------------------+-------------------+----------------------+-------------------+

- CRC (Cyclic Redundancy Check): Ensures data integrity.
- Size (Header): Enable fast forward reading.
- SeqID: Global Sequence ID
- Payload: The actual data.
- Size (Footer): Enable fast reverse reading (Startup Optimization).
*/

const (
	// Size of various components in the WAL entry header
	crcSize    = 4
	sizeSize   = 8
	seqIDSize  = 8
	headerSize = crcSize + sizeSize + seqIDSize
	footerSize = sizeSize // New Footer
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
				// Allocate buffer for Header + Payload + Footer
				return make([]byte, 4*KB+headerSize+footerSize)
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

	// Optimized: Try reverse scan first
	stat, err := f.Stat()
	if err != nil {
		return 0, err
	}
	fileSize := stat.Size()
	if fileSize >= int64(headerSize+footerSize) {
		// Read last 8 bytes (Footer)
		footer := make([]byte, footerSize)
		if _, err := f.ReadAt(footer, fileSize-int64(footerSize)); err == nil {
			lastPayloadSize := binary.BigEndian.Uint64(footer)
			// Calculate the start of the last record
			recordSize := int64(headerSize) + int64(lastPayloadSize) + int64(footerSize)
			if fileSize >= recordSize {
				// Read Header
				header := make([]byte, headerSize)
				if _, err := f.ReadAt(header, fileSize-recordSize); err == nil {
					seqID := binary.BigEndian.Uint64(header[crcSize+sizeSize : headerSize])
					// Ideally we should verify CRC here too, but for simplicity we assume closed segments are valid
					return seqID, nil
				}
			}
		}
	}

	// Fallback to sequential scan
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}
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

		// Skip payload AND footer
		if _, err := reader.Discard(int(size) + footerSize); err != nil {
			return 0, err
		}
	}
	return lastID, nil
}

// repairActiveSegment: Repair the active segment file if corrupted using Reverse Scan.
func (w *WAL) repairActiveSegment() error {
	f := w.activeSegment.file
	stat, err := f.Stat()
	if err != nil {
		return err
	}
	fileSize := stat.Size()

	// Empty file is valid
	if fileSize == 0 {
		w.activeSegment.size = 0
		w.lastSeqID = 0 // Will be fixed by loadSegments if previous segments exist
		return nil
	}

	// 1. Attempt Reverse Recovery (Optimization)
	if fileSize >= int64(headerSize+footerSize) {
		// Read Footer (Last 8 bytes)
		footer := make([]byte, footerSize)
		if _, err := f.ReadAt(footer, fileSize-int64(footerSize)); err == nil {
			payloadSize := binary.BigEndian.Uint64(footer)
			totalRecordSize := int64(headerSize) + int64(payloadSize) + int64(footerSize)

			if fileSize >= totalRecordSize {
				// Jump back to the start of this record
				offset := fileSize - totalRecordSize

				// Read Header + Payload
				data := make([]byte, headerSize+int64(payloadSize))
				if _, err := f.ReadAt(data, offset); err == nil {

					// Parse Header
					readCrc := binary.BigEndian.Uint32(data[:crcSize])
					size := binary.BigEndian.Uint64(data[crcSize : crcSize+sizeSize])
					seqID := binary.BigEndian.Uint64(data[crcSize+sizeSize : headerSize])

					if size == payloadSize {
						// Verify CRC
						verifyBuf := make([]byte, sizeSize+seqIDSize+int(payloadSize))
						binary.BigEndian.PutUint64(verifyBuf[:sizeSize], size)
						binary.BigEndian.PutUint64(verifyBuf[sizeSize:sizeSize+seqIDSize], seqID)
						copy(verifyBuf[sizeSize+seqIDSize:], data[headerSize:])

						if calculateCRC(verifyBuf) == readCrc {
							// SUCCESS: The last record is valid.
							// No truncation needed.
							w.activeSegment.size = fileSize
							w.activeSegment.writer = bufio.NewWriterSize(f, w.bufferSize)
							w.lastSeqID = seqID
							log.Printf("WAL: Startup optimized. Verified last record at offset %d (SeqID: %d)", offset, seqID)
							return nil
						}
					}
				}
			}
		}
	}

	// 2. Fallback: If reverse check fails (corrupted tail or file too small), do full scan
	log.Println("WAL: Fast startup failed or corruption detected. Falling back to sequential repair...")

	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return err
	}

	reader := bufio.NewReader(f)
	var validOffset int64 = 0
	var maxSeqID uint64 = 0

	for {
		// Read Header
		header := make([]byte, headerSize)
		if _, err := io.ReadFull(reader, header); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return err
		}

		readCrc := binary.BigEndian.Uint32(header[:crcSize])
		size := binary.BigEndian.Uint64(header[crcSize : crcSize+sizeSize])
		seqID := binary.BigEndian.Uint64(header[crcSize+sizeSize : headerSize])

		// Read Payload
		payload := make([]byte, size)
		if _, err := io.ReadFull(reader, payload); err != nil {
			break
		}

		// Read Footer
		footer := make([]byte, footerSize)
		if _, err := io.ReadFull(reader, footer); err != nil {
			break
		}
		footerSizeVal := binary.BigEndian.Uint64(footer)

		// Basic integrity check: Footer size must match Header size
		if footerSizeVal != size {
			log.Println("⚠️ WAL: Corrupted record (Size mismatch between Header and Footer)")
			break
		}

		// Verify CRC
		verifyBuf := make([]byte, sizeSize+seqIDSize+len(payload))
		binary.BigEndian.PutUint64(verifyBuf[:sizeSize], size)
		binary.BigEndian.PutUint64(verifyBuf[sizeSize:sizeSize+seqIDSize], seqID)
		copy(verifyBuf[sizeSize+seqIDSize:], payload)

		if calculateCRC(verifyBuf) != readCrc {
			log.Println("⚠️ WAL: Corrupted tail detected (CRC mismatch), truncating...")
			break
		}

		validOffset += int64(headerSize + size + footerSize)
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
	w.activeSegment.writer = bufio.NewWriterSize(f, w.bufferSize)
	w.lastSeqID = maxSeqID

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
	if dir, err := os.Open(w.dir); err == nil {
		dir.Sync()
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
				w.mu.Lock()
				if w.activeSegment == nil || w.activeSegment.file == nil {
					w.mu.Unlock()
					continue
				}
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

	// Header + Payload + Footer
	pktSize := int64(headerSize + len(payload) + footerSize)
	payloadLen := uint64(len(payload))

	currentSeqID := w.lastSeqID

	var buf []byte
	obj := w.bufPool.Get()
	if b, ok := obj.([]byte); ok && int64(cap(b)) >= pktSize {
		buf = b[:pktSize]
	} else {
		buf = make([]byte, pktSize)
	}

	// 1. Write Header: [CRC][Size][SeqID]
	binary.BigEndian.PutUint64(buf[crcSize:crcSize+sizeSize], payloadLen)
	binary.BigEndian.PutUint64(buf[crcSize+sizeSize:headerSize], currentSeqID)

	// 2. Write Payload
	copy(buf[headerSize:], payload)

	// 3. Write Footer: [Size]
	// Position = headerSize + len(payload)
	binary.BigEndian.PutUint64(buf[headerSize+len(payload):], payloadLen)

	// Calculate CRC (Header + Payload only, excluding Footer usually, but let's exclude footer from CRC for compatibility with header check)
	// We calculate CRC over [Size][SeqID][Payload] just like before
	// The buffer contains [CRC][Size][SeqID][Payload][Footer]
	// CRC is stored at buf[0:4]. It covers buf[4 : headerSize+len(payload)]

	checksum := calculateCRC(buf[crcSize : headerSize+len(payload)])
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

	pktSize := int64(headerSize + len(payload) + footerSize)
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
func (w *WAL) WriteBatch(payloads [][]byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	var batchSize int64
	for _, p := range payloads {
		batchSize += int64(headerSize + len(p) + footerSize)
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
	w.mu.Unlock()

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
func (w *WAL) TruncateFront(segmentIdx uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	var keptSegments []*Segment
	for _, seg := range w.segments {
		if seg.idx < segmentIdx {
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

// Cleanup removes closed segments that are older than the specified TTL.
func (w *WAL) CleanupByTTL(ttl time.Duration) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	threshold := time.Now().Add(-ttl)
	var keptSegments []*Segment

	for _, seg := range w.segments {
		info, err := os.Stat(seg.path)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			keptSegments = append(keptSegments, seg)
			continue
		}

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
