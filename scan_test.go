package pgxscan_test

import (
	"reflect"
	"testing"

	"github.com/guidog/pgxscan"
	"github.com/jackc/pgproto3/v2"
	"github.com/jackc/pgtype"
)

const (
	defaultDbURL = "host=localhost port=5432 dbname=testdb user=postgres sslmode=disable"
	testTable    = `CREATE TABLE scantest (
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
  xc smallint[] DEFAULT '{33,-5}',
  ya real[] DEFAULT '{13.333,-2.1}',
  yb double precision[] DEFAULT '{10000000007.333,2.10000000001}'
)`
)

type testRows struct {
	fds    []pgproto3.FieldDescription
	vals   []interface{}
	errSet error
}

func (r testRows) Err() error {
	return r.errSet
}

func (r testRows) FieldDescriptions() []pgproto3.FieldDescription {
	return r.fds
}

func (r testRows) Values() ([]interface{}, error) {
	return r.vals, nil
}

func mkTestRows() testRows {
	var (
		testFds = []pgproto3.FieldDescription{
			{Name: []byte("bigid")},
			{Name: []byte("littleid")},
			{Name: []byte("verylittleid")},
			{Name: []byte("string")},
			{Name: []byte("n")},
			{Name: []byte("r")},
			{Name: []byte("a")},
			{Name: []byte("x")},
			{Name: []byte("xx")},
			{Name: []byte("xa")},
			{Name: []byte("xb")},
			{Name: []byte("xc")},
			{Name: []byte("ya")},
			{Name: []byte("yb")},
		}
		testVals = []interface{}{
			int64(703340046535533321),
			int32(2135533321),
			int16(16384),
			string("xy"),
			float32(42.1),
			float64(-0.000001),
			pgtype.TextArray{}, // 6
			[]byte{1, 2, 3},
			pgtype.ByteaArray{},
			pgtype.Int4Array{},
			pgtype.Int8Array{},
			pgtype.Int2Array{},
			pgtype.Float4Array{},
			pgtype.Float8Array{},
		}
	)
	ta := testVals[6].(pgtype.TextArray)
	ta.Set([]string{"AA", "BB"})
	testVals[6] = ta

	ba := testVals[8].(pgtype.ByteaArray)
	ba.Set([][]byte{[]byte("0102"), []byte("x")})
	testVals[8] = ba

	i4a := testVals[9].(pgtype.Int4Array)
	i4a.Set([]int32{11, 22})
	testVals[9] = i4a

	i8a := testVals[10].(pgtype.Int8Array)
	i8a.Set([]int64{565663666322000, -566633})
	testVals[10] = i8a

	i2a := testVals[11].(pgtype.Int2Array)
	i2a.Set([]int16{33, -5})
	testVals[11] = i2a

	f4a := testVals[12].(pgtype.Float4Array)
	f4a.Set([]float32{13.333, -2.1})
	testVals[12] = f4a

	f8a := testVals[13].(pgtype.Float8Array)
	f8a.Set([]float64{10000000007.333, 2.10000000001})
	testVals[13] = f8a

	ret := testRows{
		fds:    testFds,
		vals:   testVals,
		errSet: nil,
	}

	return ret
}

func TestReadStruct(t *testing.T) {

	rows := mkTestRows()

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
	err := pgxscan.ReadStruct(nil, rows)
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
		Ya           []float32
		Yb           []float64
		Xx           [][]byte
		A            []string
		Xa           []int32
		Xb           []int64
		Xc           []int16
		// ignored fields
		bla          int64
		WaddelDaddel string
	}
	// assign values different from zero value to see if fields are untouched
	dest.bla = 7776
	dest.WaddelDaddel = "hund"

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
	if !reflect.DeepEqual(dest.Xa, []int32{11, 22}) {
		t.Error("value mismatch for field Xa")
	}
	if !reflect.DeepEqual(dest.Xb, []int64{565663666322000, -566633}) {
		t.Error("value mismatch for field Xb")
	}
	if !reflect.DeepEqual(dest.Xc, []int16{33, -5}) {
		t.Error("value mismatch for field Xc")
	}
	if !reflect.DeepEqual(dest.Ya, []float32{13.333, -2.1}) {
		t.Errorf("value mismatch for field Ya\n%v\n%v\n", dest.Ya, []float32{13.333, -2.1})
	}
	if !reflect.DeepEqual(dest.Yb, []float64{10000000007.333, 2.10000000001}) {
		t.Errorf("value mismatch for field Yb\n%v\n%v\n", dest.Yb, []float64{10000000007.333, 2.10000000001})
	}

	// ignored fields should not have changed
	if dest.bla != 7776 {
		t.Error("value mismatch for field bla")

	}
	if dest.WaddelDaddel != "hund" {
		t.Error("value mismatch for field WaddelDaddel")

	}
}

func TestReadStructEmbedded(t *testing.T) {

	rows := mkTestRows()

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
		Xa       []int32
	}
	var dest struct {
		base2
		String string
		X      []byte
		Xx     [][]byte
		A      []string
	}

	err := pgxscan.ReadStruct(&dest, rows)
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
	if !reflect.DeepEqual(dest.Xa, []int32{11, 22}) {
		t.Error("value mismatch for field Xa")
	}

}

func BenchmarkReadStruct(b *testing.B) {
	rows := mkTestRows()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
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
			Xa           []int32
			Xb           []int64
			Xc           []int16
			Ya           []float32
			Yb           []float64
			// ignored fields
			bla int64
		}
		err := pgxscan.ReadStruct(&dest, rows)
		if err != nil {
			b.Fatal(err)
		}
	}

}
