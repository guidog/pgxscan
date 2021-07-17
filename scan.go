package pgxscan

import (
	"errors"
	"fmt"
	"reflect"
	"strings"

	// "github.com/goccy/go-reflect"

	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
)

// NameMatcherFnc is the signature for a function doing the name matching for fields.
// fieldName is the name of the struct field and resultName the column name returned from the query.
// If the names match true is returned, false otherwise.
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
	// ErrInvalidDestination is returned when the destination field does not match the DB type
	ErrInvalidDestination = errors.New("destination has incompatible type")

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

	// collect all field names from struct
	structFields := make([]string, 0, 50) // preallocate, enough for most structs
	getFields(structData.Type(), &structFields)

	// field descriptions and values of result set are in sync
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
	for i := 0; i < len(fds) && len(structFields) > 0; i++ {
		fd := fds[i]
		resultName := string(fd.Name) // fd.Name is []byte
		fieldName := ""

		// match names
		for i, k := range structFields {
			if matchFnc(k, resultName) {
				// names do match
				fieldName = k
				// remove found field
				l := len(structFields) - 1
				if l > 0 {
					structFields[i] = structFields[l]
				}
				structFields = structFields[:l]
				break
			}
		}

		if len(fieldName) < 1 {
			// no matching field found, next
			continue
		}

		// do the assignment
		// named access uses the same rules as Go code
		destField := structData.FieldByName(fieldName)
		if !destField.CanSet() {
			// silently ignore fields that can not be set
			continue
		}

		// fetch value for column[i]
		v := vals[i]

		// fmt.Printf("field %s: %+v\n", fieldName, v)

		switch v := v.(type) {
		// special cases for common arrays/slices
		// fresh slices are assigned to the destination
		// TODO: improve slice handling
		case pgtype.TextArray:
			if !isStringSlice(destField) {
				return fmt.Errorf("field %s, %w", fieldName, ErrInvalidDestination)
			}
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
			if !isIntSlice(destField, 2) {
				return fmt.Errorf("field %s, %w", fieldName, ErrInvalidDestination)
			}
			// sql returned 16 bit ints
			if len(v.Dimensions) != 1 {
				return ErrNotSimpleSlice
			}
			res := make([]int16, len(v.Elements))
			for i := 0; i < len(res); i++ {
				res[i] = int16(v.Elements[i].Int)
			}
			vres := reflect.ValueOf(res)
			destField.Set(vres)
		case pgtype.Int4Array:
			if !isIntSlice(destField, 4) {
				return fmt.Errorf("field %s, %w", fieldName, ErrInvalidDestination)
			}
			// sql returned 32 bit ints
			if len(v.Dimensions) != 1 {
				return ErrNotSimpleSlice
			}
			res := make([]int32, len(v.Elements))
			for i := 0; i < len(res); i++ {
				res[i] = int32(v.Elements[i].Int)
			}
			vres := reflect.ValueOf(res)
			destField.Set(vres)
		case pgtype.Int8Array:
			if !isIntSlice(destField, 8) {
				return fmt.Errorf("field %s, %w", fieldName, ErrInvalidDestination)
			}
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
		case pgtype.Float4Array:
			if !isFloatSlice(destField, 4) {
				return fmt.Errorf("field %s, %w", fieldName, ErrInvalidDestination)
			}
			if len(v.Dimensions) != 1 {
				return ErrNotSimpleSlice
			}
			res := make([]float32, len(v.Elements))
			for i := 0; i < len(res); i++ {
				res[i] = float32(v.Elements[i].Float)
			}
			vres := reflect.ValueOf(res)
			destField.Set(vres)
		case pgtype.Float8Array:
			if !isFloatSlice(destField, 8) {
				return fmt.Errorf("field %s, %w", fieldName, ErrInvalidDestination)
			}
			if len(v.Dimensions) != 1 {
				return ErrNotSimpleSlice
			}
			res := make([]float64, len(v.Elements))
			for i := 0; i < len(res); i++ {
				res[i] = float64(v.Elements[i].Float)
			}
			vres := reflect.ValueOf(res)
			destField.Set(vres)
		case pgtype.ByteaArray:
			if !isBytesSlice(destField) {
				return fmt.Errorf("field %s, %w", fieldName, ErrInvalidDestination)
			}
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

	// see if the names are equal
	return strings.EqualFold(fieldName, resultName)
}

// helper to recursively collect all field names from the given struct
func getFields(r reflect.Type, m *[]string) {
	if r.Kind() != reflect.Struct {
		return
	}
	for i := 0; i < r.NumField(); i++ {
		field := r.Field(i)
		switch field.Type.Kind() {
		case reflect.Struct:
			getFields(field.Type, m)
		default:
			*m = append(*m, field.Name)
		}
	}
}

func isStringSlice(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Slice:
	default:
		return false
	}
	e := v.Type().Elem()
	return e.Kind() == reflect.String
}

func isBytesSlice(v reflect.Value) bool {
	if v.Kind() != reflect.Slice {
		return false
	}
	e := v.Type().Elem()
	if e.Kind() != reflect.Slice {
		return false
	}
	ee := e.Elem()
	return ee.Kind() == reflect.Uint8
}

func isIntSize(t reflect.Type, sz int) bool {
	// first check for valid int type
	// no need for uint, Postgres does not have uints.
	switch t.Kind() {
	case reflect.Int:
	case reflect.Int8:
	case reflect.Int16:
	case reflect.Int32:
	case reflect.Int64:
	default:
		return false
	}

	return int(t.Size()) == sz
}

func isIntSlice(v reflect.Value, sz int) bool {
	if v.Kind() != reflect.Slice {
		return false
	}

	e := v.Type().Elem()
	return isIntSize(e, sz)
}

func isFloatSize(t reflect.Type, sz int) bool {
	// first check for valid int type
	// no need for uint, Postgres does not have uints.
	switch t.Kind() {
	case reflect.Float32:
	case reflect.Float64:
	default:
		return false
	}

	return int(t.Size()) == sz
}

func isFloatSlice(v reflect.Value, sz int) bool {
	if v.Kind() != reflect.Slice {
		return false
	}

	e := v.Type().Elem()
	return isFloatSize(e, sz)
}
