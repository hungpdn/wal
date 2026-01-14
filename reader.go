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
	Next() bool    // Move to the next log
	Value() []byte // Get the data of the current log
	Err() error    // Get error if any
	Close() error  // Close the reader
}

// Iterator: Implementation of Reader
type Iterator struct {
	wal           *WAL
	segmentPaths  []string   // List of segment file paths
	currentIdx    int        // Index of the current segment file being read
	currentFile   *os.File   // File descriptor currently open
	currentReader *io.Reader // Reader wrapper (could be buffered)
	currentEntry  []byte     // Current log entry data
	err           error      // Error if any
	closed        bool       // Whether the iterator is closed
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
		offset := binary.BigEndian.Uint64(header[crcSize+sizeSize : headerSize])

		payload := make([]byte, size)
		if _, err := io.ReadFull(*it.currentReader, payload); err != nil {
			it.err = err
			return false
		}

		verifyBuf := make([]byte, sizeSize+offsetSize+len(payload))
		binary.BigEndian.PutUint64(verifyBuf[:sizeSize], size)
		binary.BigEndian.PutUint64(verifyBuf[sizeSize:sizeSize+offsetSize], offset)
		copy(verifyBuf[sizeSize+offsetSize:], payload)

		if calculateCRC(verifyBuf) != readCrc {
			it.err = fmt.Errorf("wal: corrupted data (crc mismatch) at segment %s", it.segmentPaths[it.currentIdx])
			return false
		}

		it.currentEntry = payload
		return true
	}
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
