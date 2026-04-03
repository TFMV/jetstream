package main

import (
	"context"
	"crypto/tls"
	"log"
	"os"

	"github.com/TFMV/jetstream/transport"
	"github.com/TFMV/jetstream/vgi"
	"github.com/quic-go/quic-go"
)

func generateTLSConfig() *tls.Config {
	certData, err := os.ReadFile("cert.pem")
	if err != nil {
		log.Fatal(err)
	}
	keyData, err := os.ReadFile("key.pem")
	if err != nil {
		log.Fatal(err)
	}
	cert, err := tls.X509KeyPair(certData, keyData)
	if err != nil {
		log.Fatal(err)
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		NextProtos:   []string{"vgi"},
	}
}

func main() {
	port := "8080"
	if p := os.Getenv("VGI_PORT"); p != "" {
		port = p
	}

	tlsConf := generateTLSConfig()
	t, err := transport.NewServer(tlsConf, "localhost:"+port)
	if err != nil {
		log.Fatalf("Failed to create transport: %v", err)
	}
	defer t.Close()

	executor, err := vgi.New("/Users/tfmv/jetstream/jetstream/cmd/server/bench.db")
	if err != nil {
		log.Fatalf("Failed to create executor: %v", err)
	}
	defer executor.Close()

	log.Printf("VGI Server listening on localhost:%s", port)

	for {
		stream, err := t.Accept(context.Background())
		if err != nil {
			log.Printf("Failed to accept stream: %v", err)
			continue
		}

		go handleStream(stream, executor)
	}
}

func handleStream(qs *quic.Stream, executor *vgi.Executor) {
	stream := &quicStreamAdapter{s: qs}
	defer stream.Close()

	queryLenBuf := make([]byte, 4)
	if _, err := stream.Read(queryLenBuf); err != nil {
		log.Printf("Failed to read query length: %v", err)
		return
	}
	queryLen := uint32(queryLenBuf[0])<<24 | uint32(queryLenBuf[1])<<16 | uint32(queryLenBuf[2])<<8 | uint32(queryLenBuf[3])

	query := make([]byte, queryLen)
	if _, err := stream.Read(query); err != nil {
		log.Printf("Failed to read query: %v", err)
		return
	}

	if err := executor.Execute(string(query), stream); err != nil {
		log.Printf("Query error: %v", err)
	}
}

type quicStreamAdapter struct {
	s *quic.Stream
}

func (a *quicStreamAdapter) SendSchema(b []byte) error {
	_, err := a.s.Write([]byte{transport.MsgTypeSchema})
	if err != nil {
		return err
	}
	var lenBuf [4]byte
	lenBuf[0] = byte(len(b) >> 24)
	lenBuf[1] = byte(len(b) >> 16)
	lenBuf[2] = byte(len(b) >> 8)
	lenBuf[3] = byte(len(b))
	_, err = a.s.Write(lenBuf[:])
	if err != nil {
		return err
	}
	_, err = a.s.Write(b)
	return err
}

func (a *quicStreamAdapter) SendRecordBatch(b []byte) error {
	_, err := a.s.Write([]byte{transport.MsgTypeRecordBatch})
	if err != nil {
		return err
	}
	var lenBuf [4]byte
	lenBuf[0] = byte(len(b) >> 24)
	lenBuf[1] = byte(len(b) >> 16)
	lenBuf[2] = byte(len(b) >> 8)
	lenBuf[3] = byte(len(b))
	_, err = a.s.Write(lenBuf[:])
	if err != nil {
		return err
	}
	_, err = a.s.Write(b)
	return err
}

func (a *quicStreamAdapter) SendEnd() error {
	_, err := a.s.Write([]byte{transport.MsgTypeEnd})
	if err != nil {
		return err
	}
	var lenBuf [4]byte
	_, err = a.s.Write(lenBuf[:])
	return err
}

func (a *quicStreamAdapter) SendError(errMsg string) error {
	_, err := a.s.Write([]byte{transport.MsgTypeError})
	if err != nil {
		return err
	}
	var lenBuf [4]byte
	l := len(errMsg)
	lenBuf[0] = byte(l >> 24)
	lenBuf[1] = byte(l >> 16)
	lenBuf[2] = byte(l >> 8)
	lenBuf[3] = byte(l)
	_, err = a.s.Write(lenBuf[:])
	if err != nil {
		return err
	}
	_, err = a.s.Write([]byte(errMsg))
	return err
}

func (a *quicStreamAdapter) Read(p []byte) (int, error) {
	return a.s.Read(p)
}

func (a *quicStreamAdapter) Recv() (byte, []byte, error) {
	var typBuf [1]byte
	if _, err := a.s.Read(typBuf[:]); err != nil {
		return 0, nil, err
	}
	var lenBuf [4]byte
	if _, err := a.s.Read(lenBuf[:]); err != nil {
		return 0, nil, err
	}
	n := uint32(lenBuf[0])<<24 | uint32(lenBuf[1])<<16 | uint32(lenBuf[2])<<8 | uint32(lenBuf[3])
	if n == 0 {
		return typBuf[0], nil, nil
	}
	payload := make([]byte, n)
	if _, err := a.s.Read(payload); err != nil {
		return 0, nil, err
	}
	return typBuf[0], payload, nil
}

func (a *quicStreamAdapter) Close() error {
	return a.s.Close()
}
