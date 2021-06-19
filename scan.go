package pgxscan

import (
	"errors"
	"reflect"
	"strings"

	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
)

var (
	ErrNotPointer     = errors.New("arg not a pointer")
	ErrNotStruct      = errors.New("arg not a struct")
	ErrDestNil        = errors.New("destination is nil")
	ErrNotSimpleArray = errors.New("db field not a simple array")
)

// ReadStruct scans the current record in rows into the given destination.
//
// The destination has to be a pointer to a struct type.
// If a struct field is exported and the name matches a returned column name the
// value of the db column is assigned to the struct field.
//
// If the struct field cannot be modified it is ignored.
func ReadStruct(dest interface{}, rows pgx.Rows) error {
	if dest == nil {
		return ErrDestNil
	}
	if rows.Err() != nil {
		return rows.Err()
	}

	err := scanStructRow(dest, rows)

	return err
}

func scanStructRow(dest interface{}, rows pgx.Rows) error {
	// check for pointer first
	t := reflect.TypeOf(dest)
	if k := t.Kind(); k != reflect.Ptr {
		return ErrNotPointer
	}

	// see if dest points to nothing
	sval := reflect.ValueOf(dest)
	if sval.IsNil() {
		return ErrDestNil
	}

	// get handle to struct after we're sure dest is a valid pointer
	structData := sval.Elem()
	if k := structData.Kind(); k != reflect.Struct {
		return ErrNotStruct
	}

	// get type of struct for field access
	structType := structData.Type()

	// collect all field names from struct
	structFields := make(map[string]struct{}, structData.NumField())
	for i := 0; i < structData.NumField(); i++ {
		name := structType.Field(i).Name
		structFields[name] = struct{}{}
	}

	// field descriptions an values are in sync
	// so fds[i] is matched by vals[i]
	fds := rows.FieldDescriptions()
	vals, err := rows.Values()
	if err != nil {
		return err
	}

	// loop over all sql values and try to find a matching struct field
	// ignore missing fields from sql result
	for i := 0; i < len(fds); i++ {
		fd := fds[i]
		fieldName := strings.Title(string(fd.Name))
		_, ok := structFields[fieldName]
		if !ok {
			continue
		}

		// field name does match
		// do the assignment
		destField := structData.FieldByName(fieldName)
		if !destField.CanSet() {
			// silently ignore fields that can not be set
			continue
		}

		v := vals[i]

		switch v := v.(type) {
		// special cases for common arrays/slices
		case pgtype.TextArray:
			if len(v.Dimensions) != 1 {
				return ErrNotSimpleArray
			}
			res := make([]string, len(v.Elements))
			for i := 0; i < len(res); i++ {
				res[i] = v.Elements[i].String
			}
			vres := reflect.ValueOf(res)
			destField.Set(vres)
		case pgtype.Int2Array:
			// sql returned 16 bit ints
			if len(v.Dimensions) != 1 {
				return ErrNotSimpleArray
			}
			res := make([]int64, len(v.Elements))
			for i := 0; i < len(res); i++ {
				res[i] = int64(v.Elements[i].Int)
			}
			vres := reflect.ValueOf(res)
			destField.Set(vres)
		case pgtype.Int4Array:
			// sql returned 32 bit ints
			if len(v.Dimensions) != 1 {
				return ErrNotSimpleArray
			}
			res := make([]int64, len(v.Elements))
			for i := 0; i < len(res); i++ {
				res[i] = int64(v.Elements[i].Int)
			}
			vres := reflect.ValueOf(res)
			destField.Set(vres)
		case pgtype.Int8Array:
			// sql returned 64 bit ints
			if len(v.Dimensions) != 1 {
				return ErrNotSimpleArray
			}
			res := make([]int64, len(v.Elements))
			for i := 0; i < len(res); i++ {
				res[i] = int64(v.Elements[i].Int)
			}
			vres := reflect.ValueOf(res)
			destField.Set(vres)
		case pgtype.ByteaArray:
			if len(v.Dimensions) != 1 {
				return ErrNotSimpleArray
			}
			res := make([][]byte, len(v.Elements))
			for i := 0; i < len(res); i++ {
				a := make([]byte, len(v.Elements[i].Bytes))
				copy(a, v.Elements[i].Bytes)
				res[i] = a
			}
			vres := reflect.ValueOf(res)
			destField.Set(vres)
		default:
			// try to make the types compatible
			// might panic in Convert
			sqlVal := reflect.ValueOf(v)
			sv := sqlVal.Convert(destField.Type())
			destField.Set(sv)
		}
	}
	return nil
}
