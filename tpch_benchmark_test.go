package benchmark

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/quic-go/quic-go"
)

/* -----------------------------
   Arrow IPC parsing utilities
------------------------------*/

func parseIPC(payload []byte) ([]arrow.Record, error) {
	reader, err := ipc.NewReader(bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	defer reader.Release()

	var out []arrow.Record

	for reader.Next() {
		rec := reader.Record()
		rec.Retain()
		out = append(out, rec)
	}

	return out, nil
}

/* -----------------------------
   QUIC query execution (stream-per-query)
------------------------------*/

// Note: changed parameter from quic.Conn (interface) to *quic.Conn (concrete type)
func runQueryOnStream(conn *quic.Conn, query string) (time.Duration, int, error) {

	stream, err := conn.OpenStreamSync(context.Background())
	if err != nil {
		return 0, 0, err
	}
	defer stream.Close()

	start := time.Now()
	rows := 0

	// ---- send query ----
	qb := []byte(query)

	if err := binary.Write(stream, binary.BigEndian, uint32(len(qb))); err != nil {
		return 0, 0, err
	}
	if _, err := stream.Write(qb); err != nil {
		return 0, 0, err
	}

	// ---- receive loop ----
	for {
		var msgType uint8
		if err := binary.Read(stream, binary.BigEndian, &msgType); err != nil {
			return time.Since(start), rows, err
		}

		var payloadLen uint32
		if err := binary.Read(stream, binary.BigEndian, &payloadLen); err != nil {
			return time.Since(start), rows, err
		}

		payload := make([]byte, payloadLen)
		if _, err := io.ReadFull(stream, payload); err != nil {
			return time.Since(start), rows, err
		}

		switch msgType {

		case 0x01, 0x02:
			records, err := parseIPC(payload)
			if err != nil {
				return 0, 0, err
			}

			for _, r := range records {
				rows += int(r.NumRows())
				r.Release()
			}

		case 0x03:
			// end-of-stream
			return time.Since(start), rows, nil

		case 0x04:
			return 0, 0, fmt.Errorf("server error: %s", string(payload))
		}
	}
}

/* -----------------------------
   Benchmark
------------------------------*/

func BenchmarkLineItemScanOverJetstream(b *testing.B) {

	query := `SELECT * FROM lineitem;`

	tlsConf := &tls.Config{
		InsecureSkipVerify: true,
		NextProtos:         []string{"vgi"},
	}

	conn, err := quic.DialAddr(context.Background(), "localhost:4242", tlsConf, nil)
	if err != nil {
		b.Fatal(err)
	}
	defer conn.CloseWithError(0, "")

	b.ResetTimer()

	var total time.Duration
	var totalRows int

	for i := 0; i < b.N; i++ {

		dur, rows, err := runQueryOnStream(conn, query)
		if err != nil {
			b.Fatal(err)
		}

		total += dur
		totalRows += rows
	}

	b.StopTimer()

	avg := total / time.Duration(b.N)

	b.ReportMetric(float64(avg.Milliseconds()), "avg_scan_ms")
	b.ReportMetric(float64(totalRows)/float64(b.N), "rows_per_query")
}
