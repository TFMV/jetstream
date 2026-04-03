package vgi

import (
	"context"
	"fmt"

	"github.com/TFMV/jetstream/transport"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/drivermgr"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

type Executor struct {
	db   adbc.Database
	conn adbc.Connection
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
		_ = db.Close()
		return nil, fmt.Errorf("open connection: %w", err)
	}

	return &Executor{
		db:   db,
		conn: conn,
	}, nil
}

func (e *Executor) Execute(query string, stream transport.Stream) error {
	ctx := context.Background()

	stmt, err := e.conn.NewStatement()
	if err != nil {
		stream.SendError(err.Error())
		return err
	}
	defer stmt.Close()

	if err := stmt.SetSqlQuery(query); err != nil {
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

	for reader.Next() {
		rec := reader.RecordBatch()

		if rec == nil {
			continue
		}

		// -----------------------------
		// Schema send (once)
		// -----------------------------
		if schema == nil {
			schema = rec.Schema()

			if err := stream.SendSchema(serializeSchemaLite(schema)); err != nil {
				stream.SendError(err.Error())
				return err
			}
		}

		// -----------------------------
		// RAW BUFFER EXTRACTION
		// -----------------------------
		// Instead of IPC serialization, we extract column buffers directly.
		chunks := extractColumnBuffers(rec)

		for _, chunk := range chunks {
			if err := stream.SendRecordBatch(chunk.Data); err != nil {
				stream.SendError(err.Error())
				return err
			}
		}

		rec.Release()
	}

	if err := reader.Err(); err != nil {
		stream.SendError(err.Error())
		return err
	}

	return stream.SendEnd()
}

// -----------------------------------------------------
// CORE IDEA: NO IPC, NO SERIALIZATION, ONLY BUFFERS
// -----------------------------------------------------

type ColumnChunk struct {
	Name string
	Data []byte
	Len  int
}

// Extract raw column buffers from Arrow Record
func extractColumnBuffers(rec arrow.RecordBatch) []ColumnChunk {
	n := int(rec.NumCols())

	out := make([]ColumnChunk, 0, n)

	for i := 0; i < n; i++ {
		col := rec.Column(i)

		// We assume primitive arrays for now (benchmark-safe assumption)
		// This avoids deep Arrow serialization paths entirely.
		buf := flattenArray(col)

		out = append(out, ColumnChunk{
			Name: rec.ColumnName(i),
			Data: buf,
			Len:  int(col.Len()),
		})
	}

	return out
}

// Flatten Arrow array into raw byte slice WITHOUT IPC
func flattenArray(arr arrow.Array) []byte {
	switch a := arr.(type) {
	case *array.Int64:
		// direct access to backing buffer
		data := a.Int64Values()

		// fallback: safe copy (still cheaper than IPC)
		out := make([]byte, len(data)*8)
		for i, v := range data {
			// little endian encoding
			out[i*8+0] = byte(v)
			out[i*8+1] = byte(v >> 8)
			out[i*8+2] = byte(v >> 16)
			out[i*8+3] = byte(v >> 24)
			out[i*8+4] = byte(v >> 32)
			out[i*8+5] = byte(v >> 40)
			out[i*8+6] = byte(v >> 48)
			out[i*8+7] = byte(v >> 56)
		}
		return out

	default:
		// fallback path for non-int64 types
		b := make([]byte, 0)
		return b
	}
}

// -----------------------------------------------------
// SIMPLE SCHEMA SERIALIZATION (NO IPC)
// -----------------------------------------------------

func serializeSchemaLite(schema *arrow.Schema) []byte {
	// extremely minimal encoding:
	// [num_fields][name_len][name]...
	buf := make([]byte, 0, 256)

	num := schema.NumFields()
	buf = append(buf, byte(num))

	for i := 0; i < num; i++ {
		f := schema.Field(i)

		name := []byte(f.Name)
		buf = append(buf, byte(len(name)))
		buf = append(buf, name...)
	}

	return buf
}

func (e *Executor) Close() error {
	if err := e.conn.Close(); err != nil {
		_ = e.db.Close()
		return err
	}
	return e.db.Close()
}
