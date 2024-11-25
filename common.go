package tiledb

/*
#include <stdlib.h>
*/
import "C"
import (
	"unsafe"
)

// scalarType includes the basic types that can be stored in a TileDB array.
// It does not include variable-sized types like strings or blobs.
// For consistency, we should arrange switch blocks in this order.
type scalarType interface {
	int | int8 | int16 | int32 | int64 |
		uint | uint8 | uint16 | uint32 | uint64 |
		float32 | float64 |
		bool
}

// slicePtr gives you an unsafe pointer to the start of a slice.
func slicePtr[T any](slc []T) unsafe.Pointer {
	return unsafe.Pointer(unsafe.SliceData(slc))
}

// cStringArray takes an array of Go strings and converts it to an array of CStrings.
// The function returned should be deferred by the caller to free allocated memory.
func cStringArray(stringList []string) ([]*C.char, func()) {
	list := make([]*C.char, len(stringList))
	for i, str := range stringList {
		list[i] = C.CString(str)
	}

	return list, func() {
		for _, str := range list {
			C.free(unsafe.Pointer(str))
		}
	}
}
