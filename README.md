# jetstream

Start the server as follows:

go run -tags="duckdb_arrow" cmd/server/main.go

Run the client as follows:

go run cmd/client/main.go "select 42"

Run the benchmark:

First create cmd/server/bench.db and generate tpch data.

 go test -bench=. -benchmem