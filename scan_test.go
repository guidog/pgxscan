package pgxscan_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/guidog/pgxscan"
	"github.com/jackc/pgx/v4"
)

const (
	defaultDbURL = "host=localhost port=5432 dbname=testdb user=postgres sslmode=disable"
	testTable    = `CREATE TABLE IF NOT EXISTS scantest (
  bigid bigint DEFAULT 7,
  string text DEFAULT 'xy',
  n real DEFAULT 42.1,
  r double precision DEFAULT -0.000001,
  a text[] DEFAULT '{"AA","BB"}',
  x bytea DEFAULT '\x010203',
  xx bytea[] DEFAULT '{"0102", "x"}',
  xa int[] DEFAULT '{11,22}'
)`
)

// helper to create a database connection
func mkConn() *pgx.Conn {
	dbUrl := os.Getenv("DB_URL")
	if len(dbUrl) < 1 {
		dbUrl = defaultDbURL
	}
	cnf, err := pgx.ParseConfig(dbUrl)
	if err != nil {
		panic(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	conn, err := pgx.ConnectConfig(ctx, cnf)
	cancel()
	if err != nil {
		panic(err)
	}
	return conn
}

func setupDB() *pgx.Conn {
	db := mkConn()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// create table for test.
	_, err := db.Exec(ctx, testTable)
	if err != nil {
		panic(err)
	}

	_, err = db.Exec(ctx, "TRUNCATE TABLE scantest")
	if err != nil {
		panic(err)
	}

	_, err = db.Exec(ctx, "INSERT INTO scantest DEFAULT VALUES")
	if err != nil {
		panic(err)
	}
	return db
}

func TestScanRow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	db := setupDB()
	defer db.Close(ctx)

	rows, err := db.Query(ctx, "SELECT * FROM scantest")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	type X struct {
		R float64
	}
	var (
		x *X
		y interface{}
	)
	y = x

	err = pgxscan.ReadStruct(y, rows)

	if err != pgxscan.ErrDestNil {
		t.Fatal("nil destination not detected")
	}

	for rows.Next() {
		var dest struct {
			Bigid int64
			Xx    [][]byte
			A     []string
			Xa    []int64
		}

		fmt.Printf("data before: %+v\n", dest)

		err = pgxscan.ReadStruct(&dest, rows)
		if err != nil {
			t.Error(err)
		}

		if dest.Bigid != 7 {
			t.Error("value mismatch for field Bigid")
		}
		fmt.Printf("data after: %+v\n", dest)
	}

}
