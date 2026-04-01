//go:build duckdb_arrow

package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"encoding/pem"
	"io"
	"log"
	"math/big"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/duckdb/duckdb-go/v2"
	"github.com/quic-go/quic-go"
)

/* -----------------------------
   TLS CERT
------------------------------*/

func generateSelfSignedCert() (tls.Certificate, error) {
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return tls.Certificate{}, err
	}

	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"VGI MVP"},
		},
		DNSNames:              []string{"localhost"},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(365 * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}

	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		return tls.Certificate{}, err
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(priv)})

	return tls.X509KeyPair(certPEM, keyPEM)
}

/* -----------------------------
   Arrow IPC
------------------------------*/

func serializeIPC(schema *arrow.Schema, record arrow.Record) ([]byte, error) {
	var buf bytes.Buffer

	w := ipc.NewWriter(&buf, ipc.WithSchema(schema))
	if err := w.Write(record); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

/* -----------------------------
   QUIC messaging (FIXED TYPES)
------------------------------*/

func sendMsg(stream *quic.Stream, msgType uint8, payload []byte) error {
	// header
	if err := binary.Write(stream, binary.BigEndian, msgType); err != nil {
		return err
	}

	pLen := uint32(len(payload))
	if err := binary.Write(stream, binary.BigEndian, pLen); err != nil {
		return err
	}

	// payload
	if pLen > 0 {
		_, err := stream.Write(payload)
		return err
	}

	return nil
}

func sendError(stream *quic.Stream, err error) {
	_ = sendMsg(stream, 0x04, []byte(err.Error()))
}

func sendEnd(stream *quic.Stream) {
	_ = sendMsg(stream, 0x03, nil)
}

/* -----------------------------
   STREAM HANDLER (FIXED)
------------------------------*/

func handleStream(stream *quic.Stream) {
	defer stream.Close()

	// keepalive safety (prevents idle timeout during scans)
	stream.SetWriteDeadline(time.Now().Add(30 * time.Second))

	// ---- read query ----
	var queryLen uint32
	if err := binary.Read(stream, binary.BigEndian, &queryLen); err != nil {
		sendError(stream, err)
		return
	}

	queryBytes := make([]byte, queryLen)
	if _, err := io.ReadFull(stream, queryBytes); err != nil {
		sendError(stream, err)
		return
	}

	query := string(queryBytes)
	log.Printf("Executing query: %s", query)

	// ---- DuckDB ----
	c, err := duckdb.NewConnector("bench.db", nil)
	if err != nil {
		sendError(stream, err)
		return
	}
	defer c.Close()

	conn, err := c.Connect(context.Background())
	if err != nil {
		sendError(stream, err)
		return
	}
	defer conn.Close()

	arrowIface, err := duckdb.NewArrowFromConn(conn)
	if err != nil {
		sendError(stream, err)
		return
	}

	rdr, err := arrowIface.QueryContext(context.Background(), query)
	if err != nil {
		sendError(stream, err)
		return
	}
	defer rdr.Release()

	// ---- streaming loop ----
	first := true
	var schema *arrow.Schema

	for rdr.Next() {
		record := rdr.Record()

		if schema == nil {
			schema = record.Schema()
		}

		payload, err := serializeIPC(schema, record)
		if err != nil {
			sendError(stream, err)
			record.Release()
			return
		}

		var msgType uint8
		if first {
			msgType = 0x01
			first = false
		} else {
			msgType = 0x02
		}

		if err := sendMsg(stream, msgType, payload); err != nil {
			record.Release()
			return
		}

		record.Release()

		// prevent QUIC idle timeout during long scans
		stream.SetWriteDeadline(time.Now().Add(30 * time.Second))
	}

	sendEnd(stream)
}

/* -----------------------------
   CONNECTION HANDLER (FIXED TYPE)
------------------------------*/

func handleConnection(conn *quic.Conn) {
	defer conn.CloseWithError(0, "")

	for {
		stream, err := conn.AcceptStream(context.Background())
		if err != nil {
			return
		}

		go handleStream(stream)
	}
}

/* -----------------------------
   MAIN
------------------------------*/

func main() {
	cert, err := generateSelfSignedCert()
	if err != nil {
		log.Fatal(err)
	}

	tlsConf := &tls.Config{
		Certificates: []tls.Certificate{cert},
		NextProtos:   []string{"vgi"},
	}

	cfg := &quic.Config{
		KeepAlivePeriod: 10 * time.Second,
	}

	listener, err := quic.ListenAddr(":4242", tlsConf, cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()

	log.Println("VGI server listening on :4242 (QUIC + DuckDB + Arrow)")

	for {
		conn, err := listener.Accept(context.Background())
		if err != nil {
			continue
		}

		go handleConnection(conn)
	}
}
