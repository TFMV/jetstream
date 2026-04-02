package benchmark

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"io"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
)

func runQuery(conn *quic.Conn, query string) (time.Duration, int, error) {
	stream, err := conn.OpenStreamSync(context.Background())
	if err != nil {
		return 0, 0, err
	}
	defer stream.Close()

	start := time.Now()

	// send query
	qb := []byte(query)
	_ = binary.Write(stream, binary.BigEndian, uint32(len(qb)))
	_, _ = stream.Write(qb)

	// read schema (ignore for benchmark)
	var msgType [1]byte
	if _, err := io.ReadFull(stream, msgType[:]); err != nil {
		return 0, 0, err
	}

	var schemaLen uint32
	if err := binary.Read(stream, binary.BigEndian, &schemaLen); err != nil {
		return 0, 0, err
	}

	schemaBuf := make([]byte, schemaLen)
	if _, err := io.ReadFull(stream, schemaBuf); err != nil {
		return 0, 0, err
	}

	totalRows := 0

	// read batches until END (0x03)
	for {
		if _, err := io.ReadFull(stream, msgType[:]); err != nil {
			return 0, 0, err
		}

		if msgType[0] == 0x03 { // End
			break
		}

		var blen uint32
		if err := binary.Read(stream, binary.BigEndian, &blen); err != nil {
			return 0, 0, err
		}

		buf := make([]byte, blen)
		if _, err := io.ReadFull(stream, buf); err != nil {
			return 0, 0, err
		}

		// we intentionally DO NOT parse Arrow
		// we only simulate row count per batch
		totalRows += len(buf) / 16
	}

	return time.Since(start), totalRows, nil
}

func BenchmarkLineItemScanNoIPC(b *testing.B) {
	query := `SELECT * FROM lineitem;`

	tlsConf := &tls.Config{
		InsecureSkipVerify: true,
		NextProtos:         []string{"vgi"},
	}

	conn, err := quic.DialAddr(context.Background(), "localhost:8080", tlsConf, nil)
	if err != nil {
		b.Fatal(err)
	}
	defer conn.CloseWithError(0, "")

	b.ReportAllocs()

	b.ResetTimer()

	var total time.Duration
	var totalRows int

	for i := 0; i < b.N; i++ {
		dur, rows, err := runQuery(conn, query)
		if err != nil {
			b.Fatal(err)
		}

		total += dur
		totalRows += rows
	}

	avg := total / time.Duration(b.N)

	b.ReportMetric(float64(avg.Milliseconds()), "avg_scan_ms")
	b.ReportMetric(float64(totalRows)/float64(b.N), "rows_per_query")
}
