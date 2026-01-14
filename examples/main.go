package main

import (
	"fmt"
	"log"
	"os"

	"github.com/hungpdn/wal"
)

func main() {
	walDir := "./wal_data"
	_ = os.RemoveAll(walDir)

	fmt.Println("🚀 WAL Library - Full Feature Demo")
	fmt.Println("==================================")

	// 1. Configuration
	// We use a very small SegmentSize (10KB) to demonstrate Log Rotation and Cleanup easily.
	cfg := wal.Config{
		BufferSize:    4 * 1024,                // 4KB Buffer
		SegmentSize:   10 * 1024,               // 10KB (Small for demo purposes)
		SegmentPrefix: "wal",                   // Prefix: wal-0000.wal
		SyncStrategy:  wal.SyncStrategyOSCache, // Performance + Safety balanced
		SyncInterval:  500,                     // Sync every 500ms
	}

	// ==========================================
	// PART 1: WRITING (Basic & Batch)
	// ==========================================
	fmt.Println("\n📝 [Part 1] Writing Data...")

	w, err := wal.Open(walDir, &cfg)
	if err != nil {
		log.Fatalf("❌ Init failed: %v", err)
	}

	// A. Basic Write
	fmt.Println("   -> Writing 500 individual entries...")
	for i := 0; i < 500; i++ {
		payload := []byte(fmt.Sprintf("Entry-%d", i))
		if err := w.Write(payload); err != nil {
			log.Fatalf("Write error: %v", err)
		}
	}

	// B. Batch Write (Higher Throughput)
	fmt.Println("   -> Writing 500 entries using WriteBatch...")
	var batch [][]byte
	for i := 500; i < 1000; i++ {
		batch = append(batch, []byte(fmt.Sprintf("BatchEntry-%d", i)))
	}
	// Writes all 500 entries acquiring the lock only once
	if err := w.WriteBatch(batch); err != nil {
		log.Fatalf("Batch write error: %v", err)
	}

	// Get current segment index to see rotation
	lastIdx := w.GetLastSegmentIdx()
	fmt.Printf("   ✅ Write complete. Current Active Segment Index: %d\n", lastIdx)

	w.Close()

	// ==========================================
	// PART 2: READING (Iterator / Checkpoint / Resume)
	// ==========================================
	fmt.Println("\n📖 [Part 2] Reading Data (Replay)...")

	wRead, err := wal.Open(walDir, &cfg)
	if err != nil {
		log.Fatalf("Open failed: %v", err)
	}
	defer wRead.Close()

	iter, err := wRead.NewReader()
	if err != nil {
		log.Fatalf("Reader failed: %v", err)
	}
	defer iter.Close()

	count := 0
	lastIndex := uint64(0)
	for iter.Next() {

		if count < 5 || count > 995 {
			fmt.Printf("ID: %d | Data: %s\n", iter.Index(), string(iter.Value()))
		}

		if iter.Index() != lastIndex+1 {
			panic("Missing gap!")
		}

		lastIndex = iter.Index()

		count++
	}

	if err := iter.Err(); err != nil {
		log.Printf("⚠️ Iterator stopped with error: %v", err)
	}
	fmt.Printf("   ✅ Read %d total records from disk.\n", count)

	fmt.Println("\n📖 [Part 2.1] Resuming from Checkpoint...")

	// Let's assume the app crashed at ID = 900 last time
	checkpointID := uint64(500)
	fmt.Printf("   -> Seeking to ID: %d ...\n", checkpointID)

	iter, err = wRead.NewReader()
	if err != nil {
		log.Fatalf("Reader failed: %v", err)
	}
	defer iter.Close()

	if found := iter.Seek(checkpointID); !found {
		if iter.Err() != nil {
			log.Fatalf("Seek failed: %v", iter.Err())
		}
		fmt.Println("   -> ID not found (end of log).")
	} else {
		fmt.Printf("   ✅ Resumed! Found ID: %d | Data: %s\n", iter.Index(), string(iter.Value()))

		for iter.Next() {
			// Process logic...
			if iter.Index() < 503 && iter.Index() > 500 {
				fmt.Printf("ID: %d | Data: %s\n", iter.Index(), string(iter.Value()))
			}
		}
	}

	// ==========================================
	// PART 3: RETENTION & CLEANUP
	// ==========================================
	fmt.Println("\n🧹 [Part 3] Retention & Cleanup...")

	// To demonstrate cleanup, we need multiple segments.
	// Since we wrote ~1000 entries with small SegmentSize, we should have multiple files.

	// A. Cleanup By Size (Keep max 50KB)
	fmt.Println("   -> Running CleanupBySize (Max 50KB)...")
	// Note: 50KB is roughly 5 segments (since we set SegmentSize=10KB)
	if err := wRead.CleanupBySize(50 * 1024); err != nil {
		log.Printf("CleanupBySize warning: %v", err)
	}

	// B. Manual Truncate (Remove everything before Segment 2)
	fmt.Println("   -> Running TruncateFront(2)...")
	if err := wRead.TruncateFront(2); err != nil {
		log.Printf("TruncateFront warning: %v", err)
	}

	// Check remaining files
	entries, _ := os.ReadDir(walDir)
	fmt.Printf("   ✅ Cleanup finished. Remaining segment files: %d\n", len(entries))
	for _, e := range entries {
		fmt.Printf("      - %s\n", e.Name())
	}

	fmt.Println("\n🎉 Demo Completed Successfully!")
}
