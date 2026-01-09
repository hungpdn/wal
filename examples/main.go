package main

import (
	"fmt"
	"log"
	"time"

	"github.com/hungpdn/wal"
)

func main() {
	walDir := "./wal_data"

	// ==========================================
	// SCENARIO 1: NORMAL DATA RECORDING
	// ==========================================
	fmt.Println("🚀 [Phase 1] Initialize WAL & Write data...")

	// 1. Config
	cfg := wal.Config{
		WALDir:       walDir,
		BufferSize:   4 * 1024,                // 4KB Buffer
		SegmentSize:  100 * 1024,              // 100KB per file (to test file rotation speed)
		SyncStrategy: wal.SyncStrategyOSCache, // Strategy 2 (Recommended)
		SyncInterval: 1000,                    // Sync every 1s
	}

	// 2. Initialize
	w, err := wal.New(cfg)
	if err != nil {
		log.Fatalf("❌ Init failed: %v", err)
	}

	// 3. Write 5000 log
	start := time.Now()
	for i := 0; i < 5000; i++ {
		payload := fmt.Sprintf("Log-Entry-%d: User A transferred $100 to B", i)
		if err := w.Write([]byte(payload)); err != nil {
			log.Fatalf("❌ Write error: %v", err)
		}
		// Simulate small delay to allow background sync to run
		if i%1000 == 0 {
			time.Sleep(10 * time.Millisecond)
		}
	}
	fmt.Printf("✅ Write finished 5000 log in %v\n", time.Since(start))

	// 4. Close securely
	if err := w.Close(); err != nil {
		log.Fatalf("❌ Close error: %v", err)
	}
	fmt.Println("🔒 WAL has been closed. Data is now safe on disk.")

	// ==========================================
	// SCENARIO 2: RECOVERY & REPLAY
	// ==========================================
	fmt.Println("\n🔄 [Phase 2] Restart & Replay Simulation...")

	// 1. Reopen WAL (Automatic Recovery if errors occur)
	w2, err := wal.New(cfg)
	if err != nil {
		log.Fatalf("❌ Recovery failed: %v", err)
	}
	defer w2.Close()

	// 2. Create Iterator to read
	iter, err := w2.NewReader()
	if err != nil {
		log.Fatalf("❌ Iterator failed: %v", err)
	}
	defer iter.Close()

	// 3. Browse logs
	count := 0
	fmt.Println("   --- Start reading ---")
	for iter.Next() {
		data := iter.Value()
		// Print the first 3 lines and the last 3 lines as a test.
		if count < 3 || count >= 4997 {
			fmt.Printf("   [%4d] %s\n", count, string(data))
		}
		if count == 3 {
			fmt.Println("   ... (reading) ...")
		}
		count++
	}

	// 4. Check for errors
	if err := iter.Err(); err != nil {
		log.Printf("⚠️ Replay stopped due to error.: %v", err)
	} else {
		fmt.Printf("✅ Replay successful: Read %d/5000 records.\n", count)
	}

	if count == 5000 {
		fmt.Println("🎉 DATA INTEGRITY 100%!")
	}
}
