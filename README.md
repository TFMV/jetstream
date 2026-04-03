# Jetstream: VGI (Vectorized Gateway Interface)

A reference implementation of an Arrow-native streaming protocol over QUIC for distributed DuckDB execution.

## Overview

This implementation demonstrates a clean separation between:
- **Transport layer**: Database-agnostic QUIC + Arrow IPC streaming
- **Execution layer**: DuckDB-backed query execution (VGI)

This architecture is designed to serve as a foundation for standardizing Arrow-native streaming over QUIC across the ecosystem.

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                  Client / Benchmark                  │
└─────────────────────┬───────────────────────────────┘
                      │ QUIC + Arrow IPC
                      ▼
┌─────────────────────────────────────────────────────┐
│   transport/transport.go - Transport Layer         │
│   - QUIC stream abstraction                         │
│   - Message framing (Schema/Batch/End)              │
│   - Buffer pooling                                  │
│   - IPCWriter/IPCReader for Arrow streaming         │
└─────────────────────┬───────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────┐
│        vgi/vgi.go - DuckDB Execution Layer          │
│   - duckdb.NewArrowFromConn() for zero-copy Arrow   │
│   - Native Arrow RecordReader → IPC → Stream        │
│   - Buffer pooling for IPC buffers                  │
└─────────────────────────────────────────────────────┘
```

## Key Design Principles

1. **Arrow-Native End-to-End**: No row-based translation; columnar data flows through the entire system
2. **Streaming First**: Results are streamed incrementally; no full materialization
3. **Composability**: Nodes can call other nodes; streams can be pipelined across services
4. **Minimal Surface Area**: Small, explicit interfaces; avoid premature abstraction

## Protocol

Messages are framed over QUIC streams:

| Type | Code | Description |
|------|------|-------------|
| Schema | 0x01 | Arrow IPC schema message |
| RecordBatch | 0x02 | Serialized Arrow record batch |
| End | 0x03 | Stream completed |
| Error | 0x04 | Error message |

## Quick Start

### Prerequisites

- Go 1.26+
- TLS certificates (`cert.pem`, `key.pem`)
- DuckDB TPCH benchmark data (`cmd/server/bench.db`)

### Build

```bash
go build -o server ./cmd/server/
```

### Run Server

```bash
./server
# Server listens on localhost:8080
```

### Run Client

```bash
go run cmd/client/main.go "SELECT * FROM lineitem LIMIT 10"
```

### Run Benchmark

```bash
# Start server first
./server &

# Run benchmark (5 second run)
go test -run=NONE -bench=BenchmarkLineItemScanNoIPC -benchtime=5s -count=1 -benchmem .
```

### Benchmark Results

```
BenchmarkLineItemScanNoIPC-10  1  3127 ms/op  65M rows  224 MB/op  6.8M allocs
```

Scanning ~65 million rows from the lineitem table over QUIC with Arrow IPC streaming using ADBC.

**Performance Improvements:**
- **Memory Usage**: Reduced from 1.3 GB to 224 MB (83% reduction) through optimized streaming and buffer pooling
- **Execution Time**: Improved from 5145 ms to 3127 ms (39% faster)
- **Allocations**: Insanely high at ~6.8M, primarily from Arrow record creation and IPC serialization

## Key Files

- `transport/transport.go` - Reusable QUIC + Arrow IPC transport layer
- `vgi/vgi.go` - DuckDB execution layer using ADBC
- `cmd/server/main.go` - Server implementation
- `cmd/client/main.go` - Client implementation
- `tpch_benchmark_test.go` - Benchmark code with TPCH data

## Environment Variables

- `VGI_PORT` - Server port (default: 8080)

## Dependencies

- `github.com/apache/arrow-adbc/go/adbc` - Arrow Database Connectivity for DuckDB
- `github.com/quic-go/quic-go` - QUIC protocol
- `github.com/apache/arrow-go/v18` - Arrow Go implementation

**Important**: Uses ADBC driver for optimized Arrow-native database access. Requires DuckDB ADBC driver to be installed on the system.

## Next Steps

1. Add `vgi_remote('addr', 'query')` function for remote query forwarding
2. Implement connection pooling for better concurrency
3. Add compression (dictionary encoding for string columns)
4. Optimize allocations further with writer reuse (API permitting)