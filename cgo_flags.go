package tiledb

// cgo concatenates all flags within a package, so just define them once here
// (https://pkg.go.dev/cmd/cgo).


/*
#cgo LDFLAGS: -ltiledb
#cgo linux LDFLAGS: -ldl
*/
import "C"
