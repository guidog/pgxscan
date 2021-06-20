// Package pgxscan adds the ability to directly scan into structs from pgx query results.
//
// Supported data types
//
// The following Go data types are supported as destinations in a struct:
//  - int64
//  - int32
//  - int16
//  - string
//  - []byte
//  - float64
//  - float32
//
// pgxscan also supports some slice types directly:
//  []int64
//  []string
//  [][]byte
//
// Only 1 dimensional arrays are supported for now.
// The slices in the struct are overwritten by newly allocated slices.
// So it does not make sense to pre-allocate anything in there.
//
// Embedded structs are supported.
// If there are duplicate field names, the highest level name is used. Which is the Go rule for access.
//
// Default name matching
//
// A match is found when the following conditions are met:
//   - both names are not empty (length > 0)
//   - the struct field is exported (uppercase first rune)
//   - the name of the struct field matches the name from the result set (EqualFold)
//
package pgxscan
