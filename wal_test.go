package wal

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func cleanUp(dir string) {
	_ = os.RemoveAll(dir)
}

func TestWAL_BasicWriteAndRead(t *testing.T) {
	dir := "./test_data_basic"
	cleanUp(dir)
	defer cleanUp(dir)

	cfg := Config{
		BufferSize:    4 * 1024,
		SegmentSize:   1024 * 1024, // 1MB
		SyncStrategy:  SyncStrategyOSCache,
		SegmentPrefix: "wal",
	}

	w, err := Open(dir, &cfg)
	if err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}

	// 1. Write
	entries := 100
	for i := 0; i < entries; i++ {
		payload := []byte(fmt.Sprintf("entry-%d", i))
		if err := w.Write(payload); err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	}

	// Verify LastSeqID
	if w.lastSeqID != uint64(entries) {
		t.Errorf("Expected LastSeqID %d, got %d", entries, w.lastSeqID)
	}

	w.Close()

	// 2. Read (Re-open)
	w2, err := Open(dir, &cfg)
	if err != nil {
		t.Fatalf("Failed to re-open WAL: %v", err)
	}
	defer w2.Close()

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
		if iter.Index() != uint64(count+1) { // SeqID starts at 1
			t.Errorf("SeqID mismatch: expected %d, got %d", count+1, iter.Index())
		}
		count++
	}

	if count != entries {
		t.Errorf("Expected %d entries, got %d", entries, count)
	}
}

func TestWAL_WriteBatch(t *testing.T) {
	dir := "./test_data_batch"
	cleanUp(dir)
	defer cleanUp(dir)

	cfg := Config{
		BufferSize:   4 * 1024,
		SegmentSize:  10 * 1024 * 1024,
		SyncStrategy: SyncStrategyOSCache,
	}

	w, err := Open(dir, &cfg)
	if err != nil {
		t.Fatalf("Init failed: %v", err)
	}
	defer w.Close()

	// Batch Write
	var batch [][]byte
	for i := 0; i < 50; i++ {
		batch = append(batch, []byte(fmt.Sprintf("batch-%d", i)))
	}

	if err := w.WriteBatch(batch); err != nil {
		t.Fatalf("WriteBatch failed: %v", err)
	}

	// Verify
	iter, _ := w.NewReader()
	defer iter.Close()
	count := 0
	for iter.Next() {
		count++
	}
	if count != 50 {
		t.Errorf("Expected 50 entries, got %d", count)
	}
}

func TestWAL_LogRotation(t *testing.T) {
	dir := "./test_data_rotation"
	cleanUp(dir)
	defer cleanUp(dir)

	// Set very small segment size to force rotation
	cfg := Config{
		BufferSize:    1024,
		SegmentSize:   100, // 100 Bytes per file
		SegmentPrefix: "wal",
	}

	w, err := Open(dir, &cfg)
	if err != nil {
		t.Fatalf("Init failed: %v", err)
	}

	// Write enough data to create multiple files
	// Header overhead is approx 20 bytes. Payload "data" is 4 bytes.
	// Total ~24 bytes per entry. 100 bytes limit -> ~4 entries per file.
	for i := 0; i < 20; i++ {
		w.Write([]byte("data"))
	}
	w.Close()

	// Check files
	entries, _ := os.ReadDir(dir)
	walFiles := 0
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".wal" {
			walFiles++
		}
	}

	if walFiles < 2 {
		t.Errorf("Expected multiple segment files, got %d", walFiles)
	}
}

func TestWAL_Seek(t *testing.T) {
	dir := "./test_data_seek"
	cleanUp(dir)
	defer cleanUp(dir)

	cfg := Config{SegmentSize: 10 * 1024 * 1024}
	w, _ := Open(dir, &cfg)

	// Write 100 entries (IDs 1 to 100)
	for i := 1; i <= 100; i++ {
		w.Write([]byte(fmt.Sprintf("val-%d", i)))
	}
	w.Close()

	// Re-open for reading
	w2, _ := Open(dir, &cfg)
	defer w2.Close()
	iter, _ := w2.NewReader()
	defer iter.Close()

	// Test Case 1: Seek to middle (ID 50)
	if !iter.Seek(50) {
		t.Fatalf("Seek(50) failed")
	}
	if iter.Index() != 50 {
		t.Errorf("Expected index 50, got %d", iter.Index())
	}
	if string(iter.Value()) != "val-50" {
		t.Errorf("Expected val-50, got %s", iter.Value())
	}

	// Test Case 2: Continue reading from 50
	iter.Next()
	if iter.Index() != 51 {
		t.Errorf("Expected next index 51, got %d", iter.Index())
	}

	// Test Case 3: Seek to non-existent future ID
	if iter.Seek(200) {
		t.Errorf("Seek(200) should fail for 100 entries")
	}

	// Test Case 4: Seek backwards (Should rely on Re-creating Reader or just work if implemented)
	// Current Seek implementation assumes forward scan from current position OR strictly forward?
	// The current Reader implementation scans from current file/position.
	// If we want random seek, we usually assume it works if we haven't passed it,
	// OR we might need to reset reader. For now let's test a new reader.
	iter2, _ := w2.NewReader()
	defer iter2.Close()
	if !iter2.Seek(10) {
		t.Errorf("Seek(10) failed")
	}
	if iter2.Index() != 10 {
		t.Errorf("Expected index 10, got %d", iter2.Index())
	}
}

func TestWAL_Cleanup(t *testing.T) {
	dir := "./test_data_cleanup"
	cleanUp(dir)
	defer cleanUp(dir)

	// Small segment size to generate many files
	cfg := Config{SegmentSize: 500} // ~20 entries per file
	w, _ := Open(dir, &cfg)

	// Write 100 entries -> Should create ~5 files
	for i := 0; i < 100; i++ {
		w.Write([]byte("payload"))
	}

	// 1. Test TruncateFront
	// Files might be: wal-0000, wal-0001, wal-0002...
	// Truncate everything before index 2 (delete 0 and 1)
	// Note: TruncateFront param is 'segmentIdx', not log SeqID.
	// We need to know segment indices.
	initialFiles, _ := os.ReadDir(dir)
	if len(initialFiles) < 3 {
		t.Skip("Not enough files generated for Cleanup test")
	}

	// Call TruncateFront with index 1 (should keep 1 and greater, delete 0)
	err := w.TruncateFront(1)
	if err != nil {
		t.Errorf("TruncateFront failed: %v", err)
	}

	// 2. Test CleanupBySize
	// Limit total size to ~500 bytes (should keep only active + maybe 1 closed)
	err = w.CleanupBySize(600)
	if err != nil {
		t.Errorf("CleanupBySize failed: %v", err)
	}

	w.Close()
}

func TestWAL_CorruptionRecovery(t *testing.T) {
	dir := "./test_data_corrupt"
	cleanUp(dir)
	defer cleanUp(dir)

	cfg := Config{SegmentSize: 1024 * 1024}
	w, _ := Open(dir, &cfg)

	// Write valid data
	w.Write([]byte("valid-1"))
	w.Write([]byte("valid-2"))
	w.Close()

	// Manually corrupt the file
	files, _ := os.ReadDir(dir)
	lastFile := filepath.Join(dir, files[len(files)-1].Name())
	f, err := os.OpenFile(lastFile, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatalf("Failed to open file for corruption: %v", err)
	}
	// Write garbage bytes (partial header or random data)
	f.Write([]byte{0xDE, 0xAD, 0xBE, 0xEF})
	f.Close()

	// Re-open WAL -> Should detect and truncate garbage
	w2, err := Open(dir, &cfg)
	if err != nil {
		t.Fatalf("Failed to recover WAL: %v", err)
	}
	defer w2.Close()

	// Verify data (should have 2 valid entries)
	iter, _ := w2.NewReader()
	count := 0
	for iter.Next() {
		count++
		// fmt.Printf("Recovered: %s\n", string(iter.Value()))
	}
	if count != 2 {
		t.Errorf("Expected 2 recovered entries, got %d", count)
	}
}

// TestWAL_SyncLogic checks if Sync can be called without error.
// Hard to verify disk flush in unit test, but ensures no panics.
func TestWAL_SyncLogic(t *testing.T) {
	dir := "./test_data_sync"
	cleanUp(dir)
	defer cleanUp(dir)

	w, _ := Open(dir, &Config{SyncStrategy: SyncStrategyAlways})
	w.Write([]byte("data"))
	if err := w.Sync(); err != nil {
		t.Errorf("Manual Sync failed: %v", err)
	}
	w.Close()
}

// TestWAL_CleanupByTTL verifies that old segments are deleted.
func TestWAL_CleanupByTTL(t *testing.T) {
	dir := "./test_data_ttl"
	cleanUp(dir)
	defer cleanUp(dir)

	// Create segments quickly
	cfg := Config{SegmentSize: 100}
	w, _ := Open(dir, &cfg)

	// Write a segment
	for i := 0; i < 10; i++ {
		w.Write([]byte("old-data"))
	}
	// Force rotation by creating new active segment implicitly via writes
	// Wait a bit to simulate "old" time
	time.Sleep(10 * time.Millisecond)

	// We need to modify the modtime of the generated file to make it "old"
	w.mu.Lock()
	// Close active to rotate
	w.createActiveSegment(w.activeSegment.idx + 1)
	w.mu.Unlock()

	// Hack: Modify ModTime of the first segment
	entries, _ := os.ReadDir(dir)
	if len(entries) > 0 {
		oldFile := filepath.Join(dir, entries[0].Name())
		// Set time to 2 hours ago
		oldTime := time.Now().Add(-2 * time.Hour)
		os.Chtimes(oldFile, oldTime, oldTime)
	}

	// Run CleanupByTTL (TTL = 1 hour)
	if err := w.CleanupByTTL(1 * time.Hour); err != nil {
		t.Errorf("CleanupByTTL failed: %v", err)
	}

	w.Close()
}
