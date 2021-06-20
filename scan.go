package pgxscan

import (
	"errors"
	"reflect"
	"strings"
	"unicode"

	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
)

// NameMatcherFnc is the signature for a function doing the name matching for fields.
// fieldName is the name of the struct field and resultName the column name returned from the query.
type NameMatcherFnc func(fieldName, resultName string) bool

var (
	// ErrNotPointer is returend when the destination is not a pointer.
	ErrNotPointer = errors.New("arg not a pointer")
	// ErrNotStruct is returned when the dereferenced destination pointer does not point to a struct.
	ErrNotStruct = errors.New("arg not a struct")
	// ErrDestNil is returned when the destination is nil or points to nothing.
	ErrDestNil = errors.New("destination is nil")
	// ErrNotSimpleSlice is returned if the destination field is a slice
	ErrNotSimpleSlice = errors.New("db field not a simple slice")
	// ErrEmptyStruct is returned if the destination struct has no fields
	ErrEmptyStruct = errors.New("destination struct has no fields")

	// DefaultNameMatcher is the matching function used by ReadStruct.
	// If not set, the internal matching is used.
	DefaultNameMatcher NameMatcherFnc = nil
)

// ReadStruct scans the current record in rows into the given destination.
//
// The destination has to be a pointer to a struct type.
// If a struct field is exported and the name matches a returned column name the
// value of the db column is assigned to the struct field.
//
// If the struct field cannot be modified it is silently ignored.
//
// ReadStruct uses DefaultNameMatcher to match struct fields to result columns.
// If it is not set, the internal matching is used.
func ReadStruct(dest interface{}, rows pgx.Rows) error {
	// bail out early if something is fishy
	if dest == nil {
		return ErrDestNil
	}
	if rows.Err() != nil {
		return rows.Err()
	}

	// check for pointer
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

	// no destination fields, return
	if structData.NumField() < 1 {
		return ErrEmptyStruct
	}

	// get type of struct for field access
	structType := structData.Type()

	// collect all field names from struct
	structFields := make(map[string]struct{}, structData.NumField())
	for i := 0; i < structData.NumField(); i++ {
		name := structType.Field(i).Name
		structFields[name] = struct{}{}
	}

	// field descriptions and values are in sync
	// so fds[i] is matched by vals[i]
	fds := rows.FieldDescriptions()
	vals, err := rows.Values()
	if err != nil {
		return err
	}

	var matchFnc NameMatcherFnc

	if DefaultNameMatcher == nil {
		matchFnc = defaultNameMatcher
	} else {
		matchFnc = DefaultNameMatcher
	}

	// loop over all sql values and try to find a matching struct field
	// ignore missing struct fields
	for i := 0; i < len(fds); i++ {
		fd := fds[i]
		resultName := string(fd.Name) // fd.Name is []byte
		fieldName := ""
		// match names
		for k := range structFields {
			if !matchFnc(k, resultName) {
				continue
			}
			// names do match
			fieldName = k
			break
		}
		if len(fieldName) < 1 {
			// no matching field found, next
			continue
		}

		// field is used, remove it
		delete(structFields, fieldName)

		// do the assignment
		destField := structData.FieldByName(fieldName)
		if !destField.CanSet() {
			// silently ignore fields that can not be set
			continue
		}

		// fetch value for column[i]
		v := vals[i]

		switch v := v.(type) {
		// special cases for common arrays/slices
		// fresh slices are assigned to the destination
		// TODO: improve slice handling
		case pgtype.TextArray:
			if len(v.Dimensions) != 1 {
				return ErrNotSimpleSlice
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
				return ErrNotSimpleSlice
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
				return ErrNotSimpleSlice
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
				return ErrNotSimpleSlice
			}
			res := make([]int64, len(v.Elements))
			for i := 0; i < len(res); i++ {
				res[i] = int64(v.Elements[i].Int)
			}
			vres := reflect.ValueOf(res)
			destField.Set(vres)
		case pgtype.ByteaArray:
			// [][]byte is bytea[] in Postgres
			if len(v.Dimensions) != 1 {
				return ErrNotSimpleSlice
			}
			res := make([][]byte, len(v.Elements))
			// need to copy bytes over
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

	return err
}

func defaultNameMatcher(fieldName, resultName string) bool {
	// empty  field name or result name always fails
	if len(fieldName) < 1 || len(resultName) < 1 {
		return false
	}

	// is struct field exported
	firstRune := []rune(fieldName)[0]
	if !unicode.IsUpper(firstRune) {
		return false
	}

	// see if the names are equal
	if !strings.EqualFold(fieldName, resultName) {
		return false
	}

	return true
}
