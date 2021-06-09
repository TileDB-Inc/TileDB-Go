// +build !jemalloc

package tiledb

import (
	"fmt"
	"unsafe"
)

// MakeSlice makes a slice of the correct type corresponding to the datatype, with a given number of elements
func (d Datatype) MakeSlice(numElements uint64) (interface{}, unsafe.Pointer, error) {
	switch d {
	case TILEDB_INT8:
		slice := make([]int8, numElements)
		return slice, unsafe.Pointer(&slice[0]), nil

	case TILEDB_INT16:
		slice := make([]int16, numElements)
		return slice, unsafe.Pointer(&slice[0]), nil

	case TILEDB_INT32:
		slice := make([]int32, numElements)
		return slice, unsafe.Pointer(&slice[0]), nil

	case TILEDB_INT64, TILEDB_DATETIME_YEAR, TILEDB_DATETIME_MONTH, TILEDB_DATETIME_WEEK, TILEDB_DATETIME_DAY, TILEDB_DATETIME_HR, TILEDB_DATETIME_MIN, TILEDB_DATETIME_SEC, TILEDB_DATETIME_MS, TILEDB_DATETIME_US, TILEDB_DATETIME_NS, TILEDB_DATETIME_PS, TILEDB_DATETIME_FS, TILEDB_DATETIME_AS:
		slice := make([]int64, numElements)
		return slice, unsafe.Pointer(&slice[0]), nil

	case TILEDB_UINT8, TILEDB_CHAR, TILEDB_STRING_ASCII, TILEDB_STRING_UTF8:
		slice := make([]uint8, numElements)
		return slice, unsafe.Pointer(&slice[0]), nil

	case TILEDB_UINT16, TILEDB_STRING_UTF16, TILEDB_STRING_UCS2:
		slice := make([]uint16, numElements)
		return slice, unsafe.Pointer(&slice[0]), nil

	case TILEDB_UINT32, TILEDB_STRING_UTF32, TILEDB_STRING_UCS4:
		slice := make([]uint32, numElements)
		return slice, unsafe.Pointer(&slice[0]), nil

	case TILEDB_UINT64:
		slice := make([]uint64, numElements)
		return slice, unsafe.Pointer(&slice[0]), nil

	case TILEDB_FLOAT32:
		slice := make([]float32, numElements)
		return slice, unsafe.Pointer(&slice[0]), nil

	case TILEDB_FLOAT64:
		slice := make([]float64, numElements)
		return slice, unsafe.Pointer(&slice[0]), nil

	default:
		return nil, nil, fmt.Errorf("error making datatype slice; unrecognized datatype: %d", d)
	}
}
