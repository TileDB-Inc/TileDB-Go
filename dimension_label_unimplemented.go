//go:build !experimental

// This file declares stub functions for experimental features in TileDB when no experimental build tag is provided.

package tiledb

/*
	#cgo LDFLAGS: -ltiledb
	#cgo linux LDFLAGS: -ldl
   	#include <stdlib.h>
*/
import "C"
import "fmt"

// HasDimensionLabel Checks whether the array schema has a dimension label of the given name.
func (a *ArraySchema) HasDimensionLabel(name string) (bool, error) {
	return false, nil
}

// getDimensionLabelDataType Retrieve a dimension label Datatype from the schema using experimental APIs.
func (q *Query) getDimensionLabelDataType(labelName string) (Datatype, error) {
	return 0, fmt.Errorf("TileDB-Go was built without experimental dimension label support.")
}
