//go:build !experimental
// +build !experimental

// This file declares stub functions for experimental features in TileDB when no experimental build tag is provided.

package tiledb

/*
	#cgo LDFLAGS: -ltiledb
	#cgo linux LDFLAGS: -ldl
   	#include <stdlib.h>
*/
import "C"
import "fmt"

// getDimensionLabelDataType Retrieve a dimension label Datatype from the schema using experimental APIs.
func (q *Query) getDimensionLabelDataType(labelName string) (Datatype, error) {
	return 0, fmt.Errorf("TileDB was built without experimental dimension label support.")
}
