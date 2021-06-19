package pgxscan_test

import (
	"context"
	"fmt"
	"time"

	"github.com/guidog/pgxscan"
)

func ExampleReadStruct() {
	const testTable = `CREATE TABLE IF NOT EXISTS scantest (
  bigid bigint DEFAULT 7,
  string text DEFAULT 'xy',
  n real DEFAULT 42.1,
  r double precision DEFAULT -0.000001,
  a text[] DEFAULT '{"AA","BB"}',
  x bytea DEFAULT '\x010203',
  xx bytea[] DEFAULT '{"0102", "x"}',
  xa int[] DEFAULT '{11,22}'
)`

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	db := setupDB()
	defer db.Close(ctx)

	rows, err := db.Query(ctx, "SELECT * FROM scantest")
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	for rows.Next() {
		var dest struct {
			String string
			X      []byte
			Bigid  int64
			N      float32
			R      float64
			Xx     [][]byte
			A      []string
			Xa     []int64
		}

		fmt.Printf("data before: %+v\n", dest)

		err = pgxscan.ReadStruct(&dest, rows)
		if err != nil {
			panic(err)
		}

		fmt.Printf("data after: %+v\n", dest)
	}
}
