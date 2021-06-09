// +build jemalloc

package tiledb

/*
#cgo LDFLAGS: /usr/local/lib/libjemalloc.a -L/usr/local/lib -Wl,-rpath,/usr/local/lib -ljemalloc -lm -lstdc++ -pthread -ldl
#include <stdlib.h>
#include <jemalloc/jemalloc.h>
*/
import "C"

import (
	"fmt"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/TileDB-Inc/TileDB-Go/bytesizes"
)

// The go:linkname directives provides backdoor access to private functions in
// the runtime. Below we're accessing the throw function.

//go:linkname throw runtime.throw
func throw(s string)

const MaxArrayLen = 1<<31 - 1

type dalloc struct {
	t  string
	sz int
}

var dallocsMu sync.Mutex
var dallocs map[unsafe.Pointer]*dalloc

func init() {
	// By initializing dallocs, we can start tracking allocations and deallocations via z.Calloc.
	dallocs = make(map[unsafe.Pointer]*dalloc)
}

// MakeSlice makes a slice of the correct type corresponding to the datatype, with a given number of elements
func (d Datatype) MakeSlice(numElements uint64) (interface{}, unsafe.Pointer, error) {
	switch d {
	case TILEDB_INT8:
		// b := Calloc(numElements * bytesizes.Int8)
		// return b, unsafe.Pointer(&b[0]), nil
		slice := make([]int8, numElements)
		return slice, unsafe.Pointer(&slice[0]), nil

	case TILEDB_INT16:
		slice := make([]int16, numElements)
		return slice, unsafe.Pointer(&slice[0]), nil

	case TILEDB_INT32:
		slice := make([]int32, numElements)
		return slice, unsafe.Pointer(&slice[0]), nil

	case TILEDB_INT64, TILEDB_DATETIME_YEAR, TILEDB_DATETIME_MONTH, TILEDB_DATETIME_WEEK, TILEDB_DATETIME_DAY, TILEDB_DATETIME_HR, TILEDB_DATETIME_MIN, TILEDB_DATETIME_SEC, TILEDB_DATETIME_MS, TILEDB_DATETIME_US, TILEDB_DATETIME_NS, TILEDB_DATETIME_PS, TILEDB_DATETIME_FS, TILEDB_DATETIME_AS:
		// Following code is an adaptation from
		// https://github.com/dgraph-io/ristretto/blob/master/z/calloc_jemalloc.go
		// Probably can use
		// "github.com/dgraph-io/ristretto/z"
		// and wrappers
		// func Calloc(size int) []byte { return z.Calloc(size, "memtest") }
		// func Free(bs []byte)         { z.Free(bs) }
		// func NumAllocBytes() int64   { return z.NumAllocBytes() }
		ptr := C.je_calloc(C.size_t(int(numElements)), C.size_t(bytesizes.Int64))
		if ptr == nil {
			// NB: throw is like panic, except it guarantees the process will be
			// terminated. The call below is exactly what the Go runtime invokes when
			// it cannot allocate memory.
			throw("out of memory")
		}

		uptr := unsafe.Pointer(ptr)
		dallocsMu.Lock()
		dallocs[uptr] = &dalloc{
			t:  "mytag",
			sz: int(numElements),
		}
		dallocsMu.Unlock()
		numBytes := int64(numElements * bytesizes.Int64)
		atomic.AddInt64(&numBytes, int64(numElements))

		// Free segfaults as of now, maybe need to manually free after usage?
		// defer func(ptr unsafe.Pointer, sz int64) {
		// 	C.je_free(ptr)
		// 	atomic.AddInt64(&numBytes, -int64(sz))
		// 	dallocsMu.Lock()
		// 	delete(dallocs, ptr)
		// 	dallocsMu.Unlock()
		// }(uptr, numBytes)

		// Interpret the C pointer as a pointer to a Go array, then slice.
		return (*[MaxArrayLen]int64)(uptr)[:numElements:numElements], uptr, nil

		// slice := make([]int64, numElements)
		// return slice, unsafe.Pointer(&slice[0]), nil

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
