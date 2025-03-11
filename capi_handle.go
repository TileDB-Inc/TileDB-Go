package tiledb

/*
#include <tiledb/tiledb.h>
*/
import "C"

import (
	"runtime"
	"sync/atomic"
	"unsafe"
)

// capiHandle encapsulates and manages the lifetime of a TileDB C API handle.
// Do not use directly; use one of the wrapper types for specialized handle kinds.
type capiHandle struct {
	ptr      unsafe.Pointer
	freeFunc func(unsafe.Pointer)
	cleanup  runtime.Cleanup
}

// Free releases the native handle held by the capiHandle.
// This method is safe to call from multiple goroutines concurrently.
// However, freeing the handle while it is being used by another goroutine is not safe and
// will result in crashes. If you cannot ensure that only one goroutine will free the handle
// after the others have finished using it, you should not use
func (x *capiHandle) Free() {
	x.cleanup.Stop()
	p := atomic.SwapPointer(&x.ptr, nil)
	// Do not fail if a handle is freed multiple times.
	if p != nil {
		x.freeFunc(p)
	}
}

// Get returns the native pointer contained in the capiHandle.
// This function will panic if it is called after calling Free.
func (x *capiHandle) Get() (ptr unsafe.Pointer) {
	ptr = atomic.LoadPointer(&x.ptr)
	if ptr == nil {
		panic("cannot use freed handle")
	}
	return
}

func newCapiHandle(p unsafe.Pointer, freeFunc func(unsafe.Pointer)) *capiHandle {
	if p == nil {
		return nil
	}
	handle := &capiHandle{
		freeFunc: freeFunc,
	}
	atomic.StorePointer(&handle.ptr, unsafe.Pointer(p))
	handle.cleanup = runtime.AddCleanup(handle, freeFunc, p)
	return handle
}

type arrayHandle struct{ *capiHandle }

func freeCapiArray(c unsafe.Pointer) { C.tiledb_array_free((**C.tiledb_array_t)(unsafe.Pointer(&c))) }

func newArrayHandle(ptr *C.tiledb_array_t) arrayHandle {
	return arrayHandle{newCapiHandle(unsafe.Pointer(ptr), freeCapiArray)}
}

func (x arrayHandle) Get() *C.tiledb_array_t {
	return (*C.tiledb_array_t)(x.capiHandle.Get())
}
