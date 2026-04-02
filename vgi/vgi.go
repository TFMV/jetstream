package vgi

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"sync"

	"github.com/TFMV/jetstream/transport"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/duckdb/duckdb-go/v2"
	_ "github.com/duckdb/duckdb-go/v2"
)

type Executor struct {
	db  *sql.DB
	mem memory.Allocator
}

func New(dbPath string) (*Executor, error) {
	db, err := sql.Open("duckdb", dbPath)
	if err != nil {
		return nil, fmt.Errorf("open duckdb: %w", err)
	}
	return &Executor{db: db, mem: memory.NewGoAllocator()}, nil
}

func (e *Executor) Execute(query string, stream transport.Stream) error {
	ctx := context.Background()

	conn, err := e.db.Conn(ctx)
	if err != nil {
		stream.SendError(err.Error())
		return err
	}
	defer conn.Close()

	var driverConn driver.Conn
	err = conn.Raw(func(c any) error {
		driverConn = c.(driver.Conn)
		return nil
	})
	if err != nil {
		stream.SendError(err.Error())
		return err
	}

	arrowAPI, err := duckdb.NewArrowFromConn(driverConn)
	if err != nil {
		stream.SendError(err.Error())
		return err
	}

	reader, err := arrowAPI.QueryContext(ctx, query)
	if err != nil {
		stream.SendError(err.Error())
		return err
	}
	defer reader.Release()

	schema := reader.Schema()

	schemaBuf := ipcBufPool.Get().(*bytes.Buffer)
	schemaBuf.Reset()
	defer ipcBufPool.Put(schemaBuf)

	schemaWriter := ipc.NewWriter(schemaBuf, ipc.WithSchema(schema))
	schemaWriter.Close()
	stream.SendSchema(schemaBuf.Bytes())

	buf := ipcBufPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer ipcBufPool.Put(buf)

	for reader.Next() {
		rec := reader.Record()
		if rec == nil {
			break
		}
		rec.Retain()

		buf.Reset()
		wr := ipc.NewWriter(buf, ipc.WithSchema(schema), ipc.WithAllocator(e.mem))
		if err := wr.Write(rec); err != nil {
			rec.Release()
			wr.Close()
			stream.SendError(err.Error())
			return err
		}
		if err := wr.Close(); err != nil {
			rec.Release()
			stream.SendError(err.Error())
			return err
		}
		rec.Release()

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
	return e.db.Close()
}

var ipcBufPool = sync.Pool{
	New: func() interface{} {
		return &bytes.Buffer{}
	},
}
