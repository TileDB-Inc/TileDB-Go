package tiledb

/*
#include <tiledb/tiledb.h>
#include <stdlib.h>
*/
import "C"

import (
	"errors"
	"math"
)

// Converts a TileDB string handle to a Go string
func stringHandleToString(str *C.tiledb_string_t) (string, error) {
	var chars *C.char
	var length C.uint64_t
	ret := C.tiledb_string_view(str, &chars, &length)
	if ret != C.TILEDB_OK {
		return "", errors.New("could not get view of string handle")
	}
	if length > math.MaxInt32 {
		return "", errors.New("string returned by TileDB is > 2GB")
	}
	return C.GoStringN(chars, C.int(length)), nil
}
