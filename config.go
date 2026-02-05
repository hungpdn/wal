package wal

import "hash/crc32"

const (
	KB = 1024    // 1 Kilobyte
	MB = KB * KB // 1 Megabyte
)

// SyncStrategy defines the synchronization strategy for WAL.
type SyncStrategy int

const (
	// Fastest but highest risk, only write to buffer, Flush and Sync every second.
	// Data loss if app crash or OS crash.
	// Default strategy.
	SyncStrategyBackground SyncStrategy = 0

	// The safest, Flush and Sync immediately on each Write.
	// Never lose data. Slowest.
	SyncStrategyAlways SyncStrategy = 1

	// Balanced, Flush to OS Cache immediately, but Sync every second. (Recommended)
	// Safe on app crash. Only lose data on OS crash/power loss.
	SyncStrategyOSCache SyncStrategy = 2
)

// Config holds the configuration for WAL.
type Config struct {
	WALDir        string       // Directory to store WAL files
	BufferSize    int          // Buffered writes size in bytes (e.g., 4KB)
	SegmentSize   int64        // Maximum size of each file (e.g., 10MB)
	SegmentPrefix string       // Prefix for segment file names (e.g., "segment")
	SyncStrategy  SyncStrategy // Sync strategy
	SyncInterval  uint         // Sync interval in milliseconds for background sync
}

// DefaultConfig provides default configuration values for WAL.
var DefaultConfig = Config{
	WALDir:        "./wal",
	BufferSize:    4 * KB,  // 4KB
	SegmentSize:   10 * MB, // 10MB
	SegmentPrefix: "segment",
	SyncStrategy:  SyncStrategyBackground,
	SyncInterval:  1000, // 1000ms = 1s
}

// SetDefault sets default values for any zero-value fields in the Config.
func (cfg *Config) SetDefault() {
	if cfg.WALDir == "" {
		cfg.WALDir = DefaultConfig.WALDir
	}
	if cfg.BufferSize == 0 {
		cfg.BufferSize = DefaultConfig.BufferSize
	}
	if cfg.SegmentSize < MB {
		cfg.SegmentSize = DefaultConfig.SegmentSize
	}
	if cfg.SegmentPrefix == "" {
		cfg.SegmentPrefix = DefaultConfig.SegmentPrefix
	}
	if cfg.SyncInterval == 0 {
		cfg.SyncInterval = DefaultConfig.SyncInterval
	}
}

// CRC32 calculation using Castagnoli polynomial
var crcTable = crc32.MakeTable(crc32.Castagnoli)

// calculateCRC computes the CRC32 checksum for the given data.
func calculateCRC(data []byte) uint32 {
	return crc32.Checksum(data, crcTable)
}
