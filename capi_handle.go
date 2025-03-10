package tiledb

/*
#include <tiledb/tiledb.h>
*/
import "C"

import (
	"runtime"
	"sync/atomic"
)

// capiHandle encapsulates and manages the lifetime of a TileDB C API handle.
type capiHandle[T any] struct {
	ptr      atomic.Pointer[T]
	freeFunc func(*T)
	cleanup  runtime.Cleanup
}

// Free releases the native handle held by the capiHandle.
// This method is safe to call from multiple goroutines concurrently.
// However, freeing the handle while it is being used by another goroutine is not safe and
// will result in crashes. If you cannot ensure that only one goroutine will free the handle
// after the others have finished using it, you should not use
func (x *capiHandle[T]) Free() {
	x.cleanup.Stop()
	p := x.ptr.Swap(nil)
	// Do not fail if a handle is freed multiple times.
	if p != nil {
		x.freeFunc(p)
	}
}

// Get returns the native pointer contained in the capiHandle.
// This function will panic if it is called after calling Free.
func (x *capiHandle[T]) Get() (ptr *T) {
	ptr = x.ptr.Load()
	if ptr == nil {
		panic("cannot use freed handle")
	}
	return
}

func newCapiHandle[T any](p *T, freeFunc func(*T)) *capiHandle[T] {
	if p == nil {
		return nil
	}
	handle := &capiHandle[T]{
		freeFunc: freeFunc,
	}
	handle.ptr.Store(p)
	handle.cleanup = runtime.AddCleanup(handle, freeFunc, p)
	return handle
}

func freeCapiArray(c *C.tiledb_array_t) { C.tiledb_array_free(&c) }

func newArrayHandle(ptr *C.tiledb_array_t) *capiHandle[C.tiledb_array_t] {
	return newCapiHandle(ptr, freeCapiArray)
}
