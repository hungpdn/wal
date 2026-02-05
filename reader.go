package wal

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

// Reader: Interface to read logs sequentially
type Reader interface {
	Next() bool          // Move to the next log
	Value() []byte       // Get the data of the current log
	Index() uint64       // Get the Global Sequence ID
	Seek(id uint64) bool // Fast Forward
	Err() error          // Get error if any
	Close() error        // Close the reader
}

// Iterator: Implementation of Reader
type Iterator struct {
	wal           *WAL
	segmentPaths  []string   // List of segment file paths
	currentIdx    int        // Index of the current segment file being read
	currentFile   *os.File   // File descriptor currently open
	currentReader *io.Reader // Reader wrapper (could be buffered)
	currentEntry  []byte     // Current log entry data
	currentSeqID  uint64     // Store current ID
	err           error      // Error if any
	closed        bool       // Whether the iterator is closed
	// Optimization: Reusable buffer for CRC verification to reduce GC pressure
	verifyBuf []byte
}

// NewReader: Create an Iterator starting from the oldest segment
func (w *WAL) NewReader() (*Iterator, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	// Combine all segment files (including closed and active files)
	var paths []string
	for _, seg := range w.segments {
		paths = append(paths, seg.path)
	}
	// Don't forget the active file (the last one)
	if w.activeSegment != nil {
		paths = append(paths, w.activeSegment.path)
	}

	return &Iterator{
		wal:          w,
		segmentPaths: paths,
		currentIdx:   0,
		currentFile:  nil, // Will open lazy in Next()
	}, nil
}

// Next: Read the next entry. Returns true if successful, false if no more data or error.
func (it *Iterator) Next() bool {
	if it.err != nil || it.closed {
		return false
	}

	// Loop to handle switching between segment files
	for {
		// If file is not opened or we've read all of the old file -> Open new file
		if it.currentFile == nil {
			if it.currentIdx >= len(it.segmentPaths) {
				return false // All files have been read
			}

			path := it.segmentPaths[it.currentIdx]
			f, err := os.Open(path)
			if err != nil {
				// If file is deleted (e.g. retention policy), we report error
				// or we could skip it using it.currentIdx++ (data loss scenario)
				// For strict consistency, we return error.
				it.err = err
				return false
			}

			it.currentFile = f
			// Create a buffer reader for faster reading
			br := bufio.NewReader(f)
			reader := io.Reader(br)
			it.currentReader = &reader
		}

		// Read Header
		header := make([]byte, headerSize)
		_, err := io.ReadFull(*it.currentReader, header)

		if err != nil {
			// If EOF is encountered, close the current file and increment the index so that the next iteration opens a new file
			if err == io.EOF {
				it.currentFile.Close()
				it.currentFile = nil
				it.currentIdx++
				continue
			}
			// If unexpected error occurs (UnexpectedEOF) -> Report error
			it.err = err
			return false
		}

		readCrc := binary.BigEndian.Uint32(header[:crcSize])
		size := binary.BigEndian.Uint64(header[crcSize : crcSize+sizeSize])
		seqID := binary.BigEndian.Uint64(header[crcSize+sizeSize : headerSize])

		payload := make([]byte, size)
		if _, err := io.ReadFull(*it.currentReader, payload); err != nil {
			it.err = err
			return false
		}

		neededSize := sizeSize + seqIDSize + len(payload)
		if cap(it.verifyBuf) < neededSize {
			it.verifyBuf = make([]byte, neededSize)
		} else {
			it.verifyBuf = it.verifyBuf[:neededSize]
		}

		binary.BigEndian.PutUint64(it.verifyBuf[:sizeSize], size)
		binary.BigEndian.PutUint64(it.verifyBuf[sizeSize:sizeSize+seqIDSize], seqID)
		copy(it.verifyBuf[sizeSize+seqIDSize:], payload)

		if calculateCRC(it.verifyBuf) != readCrc {
			it.err = fmt.Errorf("wal: corrupted data (crc mismatch) at segment %s", it.segmentPaths[it.currentIdx])
			return false
		}

		it.currentEntry = payload
		it.currentSeqID = seqID
		return true
	}
}

// Index returns the Global Sequence ID of the current log entry
func (it *Iterator) Index() uint64 {
	return it.currentSeqID
}

// Value: Get the data of the current log entry
func (it *Iterator) Value() []byte {
	// Return a copy for safety, or return the original slice if you want zero-allocation (be careful)
	out := make([]byte, len(it.currentEntry))
	copy(out, it.currentEntry)
	return out
}

// Err: Returns an error if the browsing process is interrupted
func (it *Iterator) Err() error {
	return it.err
}

// Close: Close the current file and release its resources
func (it *Iterator) Close() error {
	it.closed = true
	if it.currentFile != nil {
		return it.currentFile.Close()
	}
	return nil
}

// Seek: Fast-forward to the record with ID >= startID
// It will skip reading the payload and verifying the CRC of older records
func (it *Iterator) Seek(startID uint64) bool {
	for {

		if it.currentFile == nil {
			if it.currentIdx >= len(it.segmentPaths) {
				return false
			}
			path := it.segmentPaths[it.currentIdx]
			f, err := os.Open(path)
			if err != nil {
				it.err = err
				return false
			}
			it.currentFile = f
			br := bufio.NewReader(f)
			reader := io.Reader(br)
			it.currentReader = &reader
		}

		header := make([]byte, headerSize)
		if _, err := io.ReadFull(*it.currentReader, header); err != nil {
			if err == io.EOF {
				it.currentFile.Close()
				it.currentFile = nil
				it.currentIdx++
				continue
			}
			it.err = err
			return false
		}

		size := binary.BigEndian.Uint64(header[crcSize : crcSize+sizeSize])
		seqID := binary.BigEndian.Uint64(header[crcSize+sizeSize : headerSize])

		if seqID < startID {
			// Use Discard to jump over byte 'size' without copying the data
			if _, err := io.CopyN(io.Discard, *it.currentReader, int64(size)); err != nil {
				it.err = err
				return false
			}
			continue
		} else {

			payload := make([]byte, size)
			if _, err := io.ReadFull(*it.currentReader, payload); err != nil {
				it.err = err
				return false
			}

			verifyBuf := make([]byte, sizeSize+seqIDSize+len(payload))
			binary.BigEndian.PutUint64(verifyBuf[:sizeSize], size)
			binary.BigEndian.PutUint64(verifyBuf[sizeSize:sizeSize+seqIDSize], seqID)
			copy(verifyBuf[sizeSize+seqIDSize:], payload)

			readCrc := binary.BigEndian.Uint32(header[:crcSize])
			if calculateCRC(verifyBuf) != readCrc {
				it.err = fmt.Errorf("wal: corrupted data at seek target in segment %s", it.segmentPaths[it.currentIdx])
				return false
			}

			it.currentEntry = payload
			it.currentSeqID = seqID
			return true
		}
	}
}
