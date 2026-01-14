package wal

import (
	"fmt"
	"os"
	"testing"
)

func TestWAL_WriteAndRead(t *testing.T) {
	// 1. Setup
	dir := "./test_wal_data"
	os.RemoveAll(dir)       // Clean up trước khi test
	defer os.RemoveAll(dir) // Clean up sau khi test

	opts := Options{
		BufferSize:   4 * 1024,
		SegmentSize:  1024 * 1024, // 1MB
		SyncStrategy: SyncStrategyOSCache,
		SyncInterval: 100,
	}

	// 2. Init WAL
	w, err := Open(dir, &opts)
	if err != nil {
		t.Fatalf("Failed to init WAL: %v", err)
	}

	// 3. Write Data
	entries := 100
	for i := 0; i < entries; i++ {
		payload := []byte(fmt.Sprintf("entry-%d", i))
		if err := w.Write(payload); err != nil {
			t.Fatalf("Write failed at %d: %v", i, err)
		}
	}

	// 4. Close
	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// 5. Re-open (Recovery Test)
	w2, err := Open(dir, &opts)
	if err != nil {
		t.Fatalf("Failed to re-open WAL: %v", err)
	}
	defer w2.Close()

	// 6. Verify Data using Iterator
	iter, err := w2.NewReader()
	if err != nil {
		t.Fatalf("Failed to create reader: %v", err)
	}
	defer iter.Close()

	count := 0
	for iter.Next() {
		expected := fmt.Sprintf("entry-%d", count)
		if string(iter.Value()) != expected {
			t.Errorf("Mismatch at %d: expected %s, got %s", count, expected, string(iter.Value()))
		}
		count++
	}

	if err := iter.Err(); err != nil {
		t.Fatalf("Iterator error: %v", err)
	}

	if count != entries {
		t.Errorf("Expected %d entries, got %d", entries, count)
	}
}
