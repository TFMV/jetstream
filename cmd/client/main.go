package main

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/quic-go/quic-go"
)

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

	conn, err := quic.DialAddr(context.Background(), "localhost:8080", tlsConf, nil)
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
	qb := []byte(query)
	if err := binary.Write(stream, binary.BigEndian, uint32(len(qb))); err != nil {
		log.Fatal(err)
	}
	if _, err := stream.Write(qb); err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Query sent: %s\n", query)

	// read schema (first message from server)
	var msgType [1]byte
	if _, err := io.ReadFull(stream, msgType[:]); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("  Message type: 0x%02x\n", msgType[0])

	if msgType[0] != 0x01 { // Schema
		log.Fatalf("Expected schema message (0x01), got 0x%02x", msgType[0])
	}

	var schemaLen uint32
	if err := binary.Read(stream, binary.BigEndian, &schemaLen); err != nil {
		log.Fatal(err)
	}

	schemaBuf := make([]byte, schemaLen)
	if _, err := io.ReadFull(stream, schemaBuf); err != nil {
		log.Fatal(err)
	}

	fmt.Println("✓ SCHEMA received (raw bytes, length:", schemaLen, "):", string(schemaBuf))

	// read chunks until END (0x03)
	totalBytes := 0
	for {
		if _, err := io.ReadFull(stream, msgType[:]); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("  Message type: 0x%02x\n", msgType[0])

		if msgType[0] == 0x03 { // End
			fmt.Println("✓ END OF STREAM")
			break
		}

		if msgType[0] != 0x02 { // Chunk
			log.Fatalf("Expected chunk (0x02) or end (0x03), got 0x%02x", msgType[0])
		}

		var chunkLen uint32
		if err := binary.Read(stream, binary.BigEndian, &chunkLen); err != nil {
			log.Fatal(err)
		}

		buf := make([]byte, chunkLen)
		if _, err := io.ReadFull(stream, buf); err != nil {
			log.Fatal(err)
		}

		totalBytes += int(chunkLen)
		fmt.Printf("  Chunk received: %d bytes\n", chunkLen)
	}

	fmt.Printf("Total bytes received: %d\n", totalBytes)
}
