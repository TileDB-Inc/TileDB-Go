//go:build !go1.24

package tiledb

import (
	"runtime"
	"sync/atomic"
	"unsafe"
)

// capiHandle encapsulates and manages the lifetime of a resource, usually a TileDB C API handle.
// Do not use directly; use one of the wrapper types for specific handle kinds.
type capiHandle struct {
	ptr      unsafe.Pointer
	freeFunc func(unsafe.Pointer)
}

// Free releases the resource held by the capiHandle.
// This method is safe to call from multiple goroutines concurrently.
// However, freeing the handle while it is being used by another goroutine is not safe and
// will result in crashes.
func (x *capiHandle) Free() {
	runtime.SetFinalizer(x, nil)
	p := atomic.SwapPointer(&x.ptr, nil)
	// Do not fail if a handle is freed multiple times.
	if p != nil {
		x.freeFunc(p)
	}
}

// Get returns the pointer contained in the capiHandle.
// This function will panic if it is called after calling Free.
func (x *capiHandle) Get() (ptr unsafe.Pointer) {
	ptr = atomic.LoadPointer(&x.ptr)
	if ptr == nil {
		panic("cannot use freed handle")
	}
	return
}

func freeHandle(x *capiHandle) { x.Free() }

// newCapiHandle creates a capiHandle. It accepts a pointer and a function that will
// release the resources held by the pointer.
func newCapiHandle(p unsafe.Pointer, freeFunc func(unsafe.Pointer)) *capiHandle {
	if p == nil {
		return nil
	}
	handle := &capiHandle{
		freeFunc: freeFunc,
	}
	atomic.StorePointer(&handle.ptr, unsafe.Pointer(p))
	runtime.SetFinalizer(handle, freeHandle)
	return handle
}
