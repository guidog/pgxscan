// Package pgxscan adds the ability to directly scan into structs to pgx query results.
//
// Support for some slice types:
//  []int64
//  []string
//  [][]byte
//
// Only 1 dimensional arrays are supported for now.
package pgxscan
