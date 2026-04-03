# Jetstream: VGI (Vectorized Gateway Interface)

A reference implementation of an Arrow-native streaming protocol over QUIC for distributed DuckDB execution.

## Overview

This implementation demonstrates a clean separation between:
- **Transport layer**: Database-agnostic QUIC + raw buffer streaming
- **Execution layer**: ADBC-backed query execution with zero-copy buffer extraction

This architecture is designed to serve as a foundation for standardizing Arrow-native streaming over QUIC across the ecosystem.

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                  Client / Benchmark                  │
└─────────────────────┬───────────────────────────────┘
                      │ QUIC + Raw Buffer Streaming
                      ▼
┌─────────────────────────────────────────────────────┐
│   transport/transport.go - Transport Layer         │
│   - QUIC stream abstraction                         │
│   - Length-prefixed framing                         │
│   - Buffer pooling                                  │
│   - Raw buffer streaming                            │
└─────────────────────┬───────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────┐
│        vgi/vgi.go - ADBC Execution Layer            │
│   - ADBC driver for DuckDB                          │
│   - Raw buffer extraction from RecordBatches        │
│   - Zero-copy columnar data streaming               │
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
| Schema | 0x01 | JSON schema description |
| Chunk | 0x02 | Raw columnar buffer data |
| End | 0x03 | Stream completed |
| Error | 0x04 | Error message |

Data flows as: [schema_len][schema_json] → repeated [chunk_len][raw_buffers] → [0x03][0]

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
BenchmarkLineItemScanNoIPC-10  1  1276 ms/op  192M rows  48.8 MB/op  1.5M allocs
```

Streaming ~192 million rows from the lineitem table over QUIC with raw columnar buffer slices.

**Performance Improvements:**
- **Memory Usage**: Reduced to 48.8 MB (97% reduction from original 1.3 GB)
- **Execution Time**: 1276 ms (75% faster than original 5145 ms)
- **Allocations**: 1.5M (78% reduction from original 6.9M)
- **Throughput**: 150M rows/sec with zero-copy buffer streaming

The implementation achieves true zero-copy streaming by operating directly on DuckDB's raw columnar buffers, eliminating IPC serialization and intermediate representations.

## Key Files

- `transport/transport.go` - Reusable QUIC + raw buffer transport layer
- `vgi/vgi.go` - ADBC execution layer with raw buffer extraction
- `cmd/server/main.go` - Server implementation
- `cmd/client/main.go` - Client implementation
- `tpch_benchmark_test.go` - Benchmark code with TPCH data

## Environment Variables

- `VGI_PORT` - Server port (default: 8080)

## Dependencies

- `github.com/apache/arrow-adbc/go/adbc` - Arrow Database Connectivity for DuckDB
- `github.com/quic-go/quic-go` - QUIC protocol
- `github.com/apache/arrow-go/v18` - Arrow Go (minimal usage for buffer access)

**Important**: Uses ADBC driver for optimized columnar data access. Requires DuckDB ADBC driver to be installed on the system.

## Next Steps

1. Add `vgi_remote('addr', 'query')` function for remote query forwarding
2. Implement connection pooling for better concurrency
3. Add compression (dictionary encoding for string columns)
4. Optimize allocations further with writer reuse (API permitting)