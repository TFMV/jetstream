package vgi

import (
	"bytes"
	"context"
	"fmt"
	"sync"

	"github.com/TFMV/jetstream/transport"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/drivermgr"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

type Executor struct {
	db   adbc.Database
	conn adbc.Connection
	mem  memory.Allocator
}

func New(dbPath string) (*Executor, error) {
	var drv drivermgr.Driver

	db, err := drv.NewDatabase(map[string]string{
		"driver":  "duckdb",
		"path":    dbPath,
		"threads": "1",
	})
	if err != nil {
		return nil, fmt.Errorf("open duckdb: %w", err)
	}

	conn, err := db.Open(context.Background())
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("open connection: %w", err)
	}

	return &Executor{db: db, conn: conn, mem: memory.NewGoAllocator()}, nil
}

func (e *Executor) Execute(query string, stream transport.Stream) error {
	ctx := context.Background()

	stmt, err := e.conn.NewStatement()
	if err != nil {
		stream.SendError(err.Error())
		return err
	}
	defer stmt.Close()

	err = stmt.SetSqlQuery(query)
	if err != nil {
		stream.SendError(err.Error())
		return err
	}

	reader, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		stream.SendError(err.Error())
		return err
	}
	defer reader.Release()

	var schema *arrow.Schema
	buf := ipcBufPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer ipcBufPool.Put(buf)

	for reader.Next() {
		rec := reader.RecordBatch()
		if rec == nil {
			break
		}

		if schema == nil {
			schema = rec.Schema()
			schemaBuf := ipcBufPool.Get().(*bytes.Buffer)
			schemaBuf.Reset()
			defer ipcBufPool.Put(schemaBuf)

			schemaWriter := ipc.NewWriter(schemaBuf, ipc.WithSchema(schema))
			schemaWriter.Close()
			stream.SendSchema(schemaBuf.Bytes())
		}

		buf.Reset()
		wr := ipc.NewWriter(buf, ipc.WithSchema(schema), ipc.WithAllocator(e.mem))
		if err := wr.Write(rec); err != nil {
			wr.Close()
			stream.SendError(err.Error())
			return err
		}
		if err := wr.Close(); err != nil {
			stream.SendError(err.Error())
			return err
		}

		if err := stream.SendRecordBatch(buf.Bytes()); err != nil {
			stream.SendError(err.Error())
			return err
		}
	}

	if err := reader.Err(); err != nil {
		stream.SendError(err.Error())
		return err
	}

	return stream.SendEnd()
}

func (e *Executor) Close() error {
	if err := e.conn.Close(); err != nil {
		e.db.Close()
		return err
	}
	return e.db.Close()
}

var ipcBufPool = sync.Pool{
	New: func() interface{} {
		return &bytes.Buffer{}
	},
}
