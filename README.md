# WAL: High-Performance Write-Ahead Log in Go

![Go Version](https://img.shields.io/badge/go-1.22-blue)
![License](https://img.shields.io/badge/license-MIT-green)
[![Go Report Card](https://goreportcard.com/badge/github.com/hungpdn/wal)](https://goreportcard.com/report/github.com/hungpdn/wal)

A high-performance, concurrent-safe, and crash-resilient **Write-Ahead Log (WAL)** library for Go. Designed for building databases, message queues, or any system requiring data durability.

## Features

- 🚀 **High Performance**: Buffered I/O, optimized locking strategies and non-blocking background sync.
- 🛡️ **Data Integrity**: CRC32 checksums (Castagnoli), append-only logic and automatic corruption repair on startup.
- 🧵 **Concurrency Safe**: Thread-safe writers and readers using `sync.RWMutex` and atomic operations.
- 🔄 **Log Rotation**: Automatic segment rotation based on configurable size.
- 💾 **Flexible Sync Strategies**: Choose between Performance (Background), Safety (Always), or Balance (OSCache).
- 🔍 **Iterator API**: Memory-efficient sequential reading of logs.
- 🧵 **Crash Safety:** Automatic recovery from power failures. Detects and truncates corrupted log tails (partial writes) on startup.

## Architecture

### On-Disk Format

Each segment file consists of a sequence of binary encoded entries.

```Plaintext
+-------------------+-------------------+-------------------+----------------------+
|   CRC32 (4 bytes) |   Size (8 bytes)  |   SeqID (8 bytes) |   Payload (N bytes)  |
+-------------------+-------------------+-------------------+----------------------+
| Checksum of Data  | Length of Payload | Monotonic ID      | The actual data      |
+-------------------+-------------------+-------------------+----------------------+
```

- CRC (Cyclic Redundancy Check): Ensures data integrity.
- Size: Enable fast reading without parsing the entire file.
- SeqID: Global Sequence ID
- Payload: The actual data.

## Installation

```bash
go get github.com/hungpdn/wal
```

## Usage

### Writing Data

```go
package main

import (
 "log"
 "github.com/hungpdn/wal"
)

func main() {
 cfg := wal.Config{
    SegmentSize:  10 * 1024 * 1024, // 10MB
    SyncStrategy: wal.SyncStrategyOSCache,
 }

 w, _ := wal.Open("./wal_data", &cfg)
 defer w.Close()

 // Write data
 w.Write([]byte("Hello WAL"))
 w.Write([]byte("Another log entry"))
}
```

### Reading Data (Replay)

```go
w, _ := wal.Open("", &cfg) // Auto-recovers on open

iter, _ := w.NewReader()
defer iter.Close()

for iter.Next() {
   data := iter.Value()
   log.Printf("Log Data: %s", string(data))
}

if err := iter.Err(); err != nil {
   log.Printf("Error reading wal: %v", err)
}
```

### Config

|Field       |Type  |Default   |Description                                         |
|------------|------|----------|----------------------------------------------------|
|SegmentSize |int64 |10MB      |Max size of a single segment file before rotation.  |
|BufferSize  |int   |4KB       |Size of the in-memory buffer.                       |
|SyncStrategy|int   |Background|0: Background, 1: Always (Fsync), 2: OSCache (Recm).|
|SyncInterval|uint  |1000ms    |Interval for background sync execution.             |
|Mode        |int   |0         |0: debug, 1: prod.                                  |

### Sync Strategies

- SyncStrategyBackground (0): Fastest. Writes to buffer. OS handles disk sync. Risk of data loss on OS crash.
- SyncStrategyAlways (1): Safest. fsync on every write. Slowest performance.
- SyncStrategyOSCache (2): Recommended. Flushes to OS cache immediately, background fsync every interval. Safe against app crashes, slight risk on power loss.

## Contributing

Contributions are welcome! Please fork the repository and open a pull request.

1. Fork the Project.
2. Create your Feature Branch (`git checkout -b feature/AmazingFeature`)
3. Commit your Changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the Branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## License

MIT License. See [LICENSE](LICENSE) file.

## TODO

- [ ] Log level, metrics
- [x] Tests
- [ ] Retention policy (remove, upload to s3, gcs, etc)
- [ ] Index
- [x] CI
- [ ] Benchmarks
- [ ] Documentation
- [x] Open source template (makefile, license, code of conduct, contributing, etc)
- [ ] Distributed Replication

## Reference

- [tidwall/wal](https://github.com/tidwall/wal)
- [Log: What Every Software Engineer Should Know About Real-Time Data's Unifying Abstraction](https://engineering.linkedin.com/distributed-systems/log-what-every-software-engineer-should-know-about-real-time-datas-unifying)
