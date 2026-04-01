package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/quic-go/quic-go"
)

func parseIPC(payload []byte) (*arrow.Schema, []arrow.RecordBatch, error) {
	reader, err := ipc.NewReader(bytes.NewReader(payload))
	if err != nil {
		return nil, nil, err
	}
	defer reader.Release()

	schema := reader.Schema()
	var batches []arrow.RecordBatch
	for reader.Next() {
		rec := reader.Record()
		batches = append(batches, rec)
	}
	return schema, batches, nil
}

func printBatch(record arrow.RecordBatch) {
	fmt.Printf("  Batch: %d rows, %d columns\n", record.NumRows(), record.NumCols())
	if record.NumRows() == 0 {
		return
	}
	if record.NumRows() > 20 {
		fmt.Println("    (large - summary only)")
		return
	}
	for i := 0; i < int(record.NumCols()); i++ {
		col := record.Column(i)
		field := record.Schema().Field(i)
		fmt.Printf("    %s: %s\n", field.Name, col)
	}
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run client.go \"SQL QUERY\"")
		os.Exit(1)
	}
	query := os.Args[1]

	tlsConf := &tls.Config{
		InsecureSkipVerify: true,
		NextProtos:         []string{"vgi"},
	}

	conn, err := quic.DialAddr(context.Background(), "localhost:4242", tlsConf, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.CloseWithError(0, "")

	stream, err := conn.OpenStreamSync(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	defer stream.Close()

	// send query
	queryBytes := []byte(query)
	if err := binary.Write(stream, binary.BigEndian, uint32(len(queryBytes))); err != nil {
		log.Fatal(err)
	}
	if _, err := stream.Write(queryBytes); err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Query sent: %s\n", query)

	for {
		var msgType uint8
		if err := binary.Read(stream, binary.BigEndian, &msgType); err != nil {
			break
		}

		var payloadLen uint32
		if err := binary.Read(stream, binary.BigEndian, &payloadLen); err != nil {
			break
		}

		payload := make([]byte, payloadLen)
		if _, err := io.ReadFull(stream, payload); err != nil {
			break
		}

		switch msgType {
		case 0x01: // SCHEMA
			fmt.Println("✓ SCHEMA received")
			schema, batches, err := parseIPC(payload)
			if err != nil {
				fmt.Println("  parse error:", err)
				continue
			}
			fmt.Println("  Schema:", schema)
			for _, b := range batches {
				printBatch(b)
				b.Release()
			}
		case 0x02: // RECORD_BATCH
			fmt.Println("✓ RECORD_BATCH received")
			_, batches, err := parseIPC(payload)
			if err != nil {
				fmt.Println("  parse error:", err)
				continue
			}
			for _, b := range batches {
				printBatch(b)
				b.Release()
			}
		case 0x03: // END
			fmt.Println("✓ END OF STREAM")
			return
		case 0x04: // ERROR
			fmt.Printf("✗ ERROR: %s\n", string(payload))
			return
		}
	}
}
