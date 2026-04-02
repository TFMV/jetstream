package transport

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/quic-go/quic-go"
)

const (
	MsgTypeSchema      = 0x01
	MsgTypeRecordBatch = 0x02
	MsgTypeEnd         = 0x03
	MsgTypeError       = 0x04
)

type Stream interface {
	SendSchema(schemaIPCBuf []byte) error
	SendRecordBatch(batchIPCBuf []byte) error
	SendEnd() error
	SendError(errMsg string) error
	Recv() (msgType byte, payload []byte, err error)
	Close() error
}

type quicStream struct {
	s       *quic.Stream
	mu      sync.Mutex
	rb      []byte
	readBuf sync.Pool
}

func NewQUICStream(s *quic.Stream) *quicStream {
	return &quicStream{
		s: s,
		readBuf: sync.Pool{
			New: func() interface{} {
				b := make([]byte, 4096)
				return &b
			},
		},
	}
}

func (q *quicStream) SendSchema(b []byte) error {
	return q.sendMsg(MsgTypeSchema, b)
}

func (q *quicStream) SendRecordBatch(b []byte) error {
	return q.sendMsg(MsgTypeRecordBatch, b)
}

func (q *quicStream) SendEnd() error {
	return q.sendMsg(MsgTypeEnd, nil)
}

func (q *quicStream) SendError(errMsg string) error {
	return q.sendMsg(MsgTypeError, []byte(errMsg))
}

func (q *quicStream) sendMsg(typ byte, payload []byte) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if _, err := q.s.Write([]byte{typ}); err != nil {
		return fmt.Errorf("write type: %w", err)
	}

	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(payload)))
	if _, err := q.s.Write(lenBuf[:]); err != nil {
		return fmt.Errorf("write len: %w", err)
	}

	if len(payload) > 0 {
		if _, err := q.s.Write(payload); err != nil {
			return fmt.Errorf("write payload: %w", err)
		}
	}
	return nil
}

func (q *quicStream) Recv() (byte, []byte, error) {
	var typBuf [1]byte
	if _, err := q.s.Read(typBuf[:]); err != nil {
		return 0, nil, fmt.Errorf("read type: %w", err)
	}

	var lenBuf [4]byte
	if _, err := q.s.Read(lenBuf[:]); err != nil {
		return 0, nil, fmt.Errorf("read len: %w", err)
	}

	n := binary.BigEndian.Uint32(lenBuf[:])
	if n == 0 {
		return typBuf[0], nil, nil
	}

	pb := q.readBuf.Get().(*[]byte)
	buf := *pb
	if cap(buf) < int(n) {
		buf = make([]byte, n)
	} else {
		buf = buf[:n]
	}
	defer q.readBuf.Put(pb)

	if _, err := io.ReadFull(q.s, buf); err != nil {
		return 0, nil, fmt.Errorf("read payload: %w", err)
	}
	return typBuf[0], buf, nil
}

func (q *quicStream) Close() error {
	return q.s.Close()
}

type Transport struct {
	listener *quic.Listener
	conn     *quic.Conn
	mem      memory.Allocator
}

func NewServer(tlsConf *tls.Config, addr string) (*Transport, error) {
	l, err := quic.ListenAddr(addr, tlsConf, nil)
	if err != nil {
		return nil, fmt.Errorf("quic listen: %w", err)
	}
	return &Transport{listener: l, mem: memory.NewGoAllocator()}, nil
}

func NewClient(addr string, tlsConf *tls.Config) (*Transport, error) {
	c, err := quic.DialAddr(context.Background(), addr, tlsConf, nil)
	if err != nil {
		return nil, fmt.Errorf("quic dial: %w", err)
	}
	return &Transport{conn: c, mem: memory.NewGoAllocator()}, nil
}

func (t *Transport) Accept(ctx context.Context) (*quic.Stream, error) {
	conn, err := t.listener.Accept(ctx)
	if err != nil {
		return nil, err
	}
	return conn.AcceptStream(ctx)
}

func (t *Transport) OpenStream(ctx context.Context) (Stream, error) {
	qs, err := t.conn.OpenStreamSync(ctx)
	if err != nil {
		return nil, err
	}
	return NewQUICStream(qs), nil
}

func (t *Transport) Close() error {
	if t.listener != nil {
		return t.listener.Close()
	}
	if t.conn != nil {
		t.conn.CloseWithError(0, "")
	}
	return nil
}

func (t *Transport) Allocator() memory.Allocator {
	return t.mem
}

type IPCWriter struct {
	stream     Stream
	schema     *arrow.Schema
	schemaSent bool
	mu         sync.Mutex
	buf        *bytes.Buffer
}

var ipcBufPool = sync.Pool{
	New: func() interface{} {
		return &bytes.Buffer{}
	},
}

func NewIPCWriter(stream Stream, schema *arrow.Schema) *IPCWriter {
	buf := ipcBufPool.Get().(*bytes.Buffer)
	buf.Reset()
	return &IPCWriter{
		stream: stream,
		schema: schema,
		buf:    buf,
	}
}

func (w *IPCWriter) Write(record arrow.Record) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if !w.schemaSent {
		w.buf.Reset()
		wr := ipc.NewWriter(w.buf, ipc.WithSchema(w.schema))
		wr.Close()
		w.stream.SendSchema(w.buf.Bytes())
		w.schemaSent = true
	}

	w.buf.Reset()
	wr := ipc.NewWriter(w.buf, ipc.WithSchema(w.schema))

	record.Retain()
	if err := wr.Write(record); err != nil {
		record.Release()
		wr.Close()
		return fmt.Errorf("write record: %w", err)
	}
	if err := wr.Close(); err != nil {
		record.Release()
		return fmt.Errorf("close writer: %w", err)
	}
	record.Release()

	return w.stream.SendRecordBatch(w.buf.Bytes())
}

func (w *IPCWriter) Close() error {
	ipcBufPool.Put(w.buf)
	return w.stream.SendEnd()
}

type streamWriter struct {
	stream Stream
}

func (sw *streamWriter) Write(p []byte) (int, error) {
	if err := sw.stream.SendRecordBatch(p); err != nil {
		return 0, err
	}
	return len(p), nil
}

func (sw *streamWriter) Close() error {
	return nil
}

type IPCReader struct {
	stream Stream
	reader *ipc.Reader
	rec    arrow.Record
}

func NewIPCReader(stream Stream) (*IPCReader, error) {
	typ, payload, err := stream.Recv()
	if err != nil {
		return nil, fmt.Errorf("recv schema: %w", err)
	}
	if typ != MsgTypeSchema {
		return nil, fmt.Errorf("expected schema (0x01), got 0x%02x", typ)
	}

	r, err := ipc.NewReader(bytes.NewReader(payload), ipc.WithAllocator(memory.NewGoAllocator()))
	if err != nil {
		return nil, fmt.Errorf("create reader: %w", err)
	}

	return &IPCReader{stream: stream, reader: r}, nil
}

func (r *IPCReader) Schema() *arrow.Schema {
	return r.reader.Schema()
}

func (r *IPCReader) Next() bool {
	rec, err := r.reader.Read()
	if err != nil {
		if err == io.EOF {
			return false
		}
		r.rec = nil
		return false
	}
	r.rec = rec
	return true
}

func (r *IPCReader) Record() arrow.Record {
	return r.rec
}

func (r *IPCReader) Close() error {
	if r.rec != nil {
		r.rec.Release()
	}
	typ, _, err := r.stream.Recv()
	if err != nil {
		return fmt.Errorf("recv end: %w", err)
	}
	if typ != MsgTypeEnd {
		return fmt.Errorf("expected end (0x03), got 0x%02x", typ)
	}
	return nil
}
