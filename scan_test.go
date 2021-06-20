package pgxscan_test

import (
	"context"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/guidog/pgxscan"
	"github.com/jackc/pgx/v4"
)

const (
	defaultDbURL = "host=localhost port=5432 dbname=testdb user=postgres sslmode=disable"
	testTable    = `CREATE TABLE IF NOT EXISTS scantest (
  bigid bigint DEFAULT 703340046535533321,
  littleid int DEFAULT 2135533321,
  verylittleid smallint DEFAULT 16384,
  string text DEFAULT 'xy',
  n real DEFAULT 42.1,
  r double precision DEFAULT -0.000001,
  a text[] DEFAULT '{"AA","BB"}',
  x bytea DEFAULT '\x010203',
  xx bytea[] DEFAULT '{"0102", "x"}',
  xa int[] DEFAULT '{11,22}',
  xb bigint[] DEFAULT '{565663666322000,-566633}',
  xc smallint[] DEFAULT '{33,-5}'
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

func TestReadStruct(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	db := setupDB()
	defer db.Close(ctx)

	rows, err := db.Query(ctx, "SELECT * FROM scantest")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	if found := rows.Next(); !found {
		t.Fatal("no test data found")
	}

	type X struct {
		R float64
	}
	var (
		w X
		x *X
		y interface{}
		z struct{}
	)
	y = x

	// check if nil pointer is detected
	err = pgxscan.ReadStruct(nil, rows)
	if err != pgxscan.ErrDestNil {
		t.Fatal("nil pointer not detected")
	}

	// check non-struct is detected
	err = pgxscan.ReadStruct(&w.R, rows)
	if err != pgxscan.ErrNotStruct {
		t.Fatal("non-struct not detected")
	}

	// check non-pointer is detected
	err = pgxscan.ReadStruct(w, rows)
	if err != pgxscan.ErrNotPointer {
		t.Fatal("non-pointer not detected")
	}

	// check if nil reference is detected
	err = pgxscan.ReadStruct(y, rows)
	if err != pgxscan.ErrDestNil {
		t.Fatal("nil destination not detected")
	}

	// empty destination struct
	err = pgxscan.ReadStruct(&z, rows)
	if err != pgxscan.ErrEmptyStruct {
		t.Fatal("struct{} destination not detected")
	}

	// type w/ supported data types
	// field order is not relevant
	var dest struct {
		String       string
		X            []byte
		Bigid        int64
		LittleId     int32
		VeryLittleId int16
		N            float32
		R            float64
		Xx           [][]byte
		A            []string
		Xa           []int64
		Xb           []int64
		Xc           []int64
		// ignored fields
		bla int64
	}

	err = pgxscan.ReadStruct(&dest, rows)
	if err != nil {
		t.Error(err)
	}

	if dest.String != "xy" {
		t.Error("value mismatch for field String")
	}
	if !reflect.DeepEqual(dest.X, []byte{1, 2, 3}) {
		t.Error("value mismatch for field X")
	}
	if dest.Bigid != 703340046535533321 {
		t.Error("value mismatch for field Bigid")
	}
	if dest.LittleId != 2135533321 {
		t.Error("value mismatch for field LittleId")
	}
	if dest.VeryLittleId != 16384 {
		t.Error("value mismatch for field VeryLittleId")
	}
	if dest.N != float32(42.1) {
		t.Error("value mismatch for field N")
	}
	if dest.R != float64(-0.000001) {
		t.Error("value mismatch for field R")
	}
	if !reflect.DeepEqual(dest.Xx, [][]byte{[]byte("0102"), []byte("x")}) {
		t.Error("value mismatch for field Xx")
	}
	if !reflect.DeepEqual(dest.A, []string{"AA", "BB"}) {
		t.Error("value mismatch for field A")
	}
	if !reflect.DeepEqual(dest.Xa, []int64{11, 22}) {
		t.Error("value mismatch for field Xa")
	}
	if !reflect.DeepEqual(dest.Xb, []int64{565663666322000, -566633}) {
		t.Error("value mismatch for field Xb")
	}
	if !reflect.DeepEqual(dest.Xc, []int64{33, -5}) {
		t.Error("value mismatch for field Xc")
	}

}

func TestReadStructEmbedded(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	db := setupDB()
	defer db.Close(ctx)

	rows, err := db.Query(ctx, "SELECT * FROM scantest")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	if found := rows.Next(); !found {
		t.Fatal("no test data found")
	}

	// type w/ supported data types
	// field order is not relevant
	type base1 struct {
		A            []string
		Bigid        int64
		VeryLittleId int16
	}
	type base2 struct {
		base1
		LittleId int32
		N        float32
		R        float64
		A        []string
		Xa       []int64
	}
	var dest struct {
		base2
		String string
		X      []byte
		Xx     [][]byte
		A      []string
	}

	err = pgxscan.ReadStruct(&dest, rows)
	if err != nil {
		t.Error(err)
	}

	// fmt.Printf("result: %+v\n", dest)

	if dest.String != "xy" {
		t.Error("value mismatch for field String")
	}
	if !reflect.DeepEqual(dest.X, []byte{1, 2, 3}) {
		t.Error("value mismatch for field X")
	}
	if dest.Bigid != 703340046535533321 {
		t.Error("value mismatch for field Bigid")
	}
	if dest.LittleId != 2135533321 {
		t.Error("value mismatch for field LittleId")
	}
	if dest.VeryLittleId != 16384 {
		t.Error("value mismatch for field VeryLittleId")
	}
	if dest.N != float32(42.1) {
		t.Error("value mismatch for field N")
	}
	if dest.R != float64(-0.000001) {
		t.Error("value mismatch for field R")
	}
	if !reflect.DeepEqual(dest.Xx, [][]byte{[]byte("0102"), []byte("x")}) {
		t.Error("value mismatch for field Xx")
	}
	if !reflect.DeepEqual(dest.A, []string{"AA", "BB"}) {
		t.Error("value mismatch for field A")
	}
	if !reflect.DeepEqual(dest.Xa, []int64{11, 22}) {
		t.Error("value mismatch for field Xa")
	}

}
