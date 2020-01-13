package tiledb

/*
#cgo LDFLAGS: -ltiledb
#cgo linux LDFLAGS: -ldl
#include <tiledb/tiledb.h>
#include <stdlib.h>
*/
import "C"
import (
	"fmt"
	"runtime"
	"unsafe"
)

// Buffer A generic Buffer object used by some TileDB APIs
type Buffer struct {
	tiledbBuffer *C.tiledb_buffer_t
	context      *Context
}

// NewBuffer Allocs a new buffer
func NewBuffer(context *Context) (*Buffer, error) {
	buffer := Buffer{context: context}

	ret := C.tiledb_buffer_alloc(buffer.context.tiledbContext, &buffer.tiledbBuffer)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error creating tiledb buffer: %s", buffer.context.LastError())
	}

	// Set finalizer for free C pointer on gc
	runtime.SetFinalizer(&buffer, func(buffer *Buffer) {
		buffer.Free()
	})

	return &buffer, nil
}

// Free c-alloc'ed data types
func (b *Buffer) Free() {
	if b.tiledbBuffer != nil {
		C.tiledb_buffer_free(&b.tiledbBuffer)
	}
}

// SetType sets buffer datatype
func (b *Buffer) SetType(datatype Datatype) error {
	ret := C.tiledb_buffer_set_type(b.context.tiledbContext, b.tiledbBuffer, C.tiledb_datatype_t(datatype))
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error setting datatype for tiledb buffer: %s", b.context.LastError())
	}
	return nil
}

// Type returns the buffer datatype
func (b *Buffer) Type() (Datatype, error) {
	var bufferType C.tiledb_datatype_t
	ret := C.tiledb_buffer_get_type(b.context.tiledbContext, b.tiledbBuffer, &bufferType)

	if ret != C.TILEDB_OK {
		return 0, fmt.Errorf("Error getting tiledb buffer type: %s", b.context.LastError())
	}

	return Datatype(bufferType), nil
}

// Data returns a byte slice backed by the underlying C memory region of the buffer
func (b *Buffer) Data() ([]byte, error) {
	var cbuffer unsafe.Pointer
	var csize C.uint64_t

	ret := C.tiledb_buffer_get_data(b.context.tiledbContext, b.tiledbBuffer, &cbuffer, &csize)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error getting tiledb buffer data: %s", b.context.LastError())
	}

	if cbuffer == nil {
		return nil, nil
	} else {
		size := uint64(csize)
		return (*[1 << 46]uint8)(unsafe.Pointer(cbuffer))[:size:size], nil
	}
}

// SetType sets the data pointer and size on the Buffer to the given slice
func (b *Buffer) SetBuffer(buffer []byte) error {
	bufferSize := len(buffer)

	ret := C.tiledb_buffer_set_data(b.context.tiledbContext, b.tiledbBuffer, unsafe.Pointer(&buffer[0]), C.uint64_t(bufferSize))
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error setting tiledb buffer: %s", b.context.LastError())
	}

	return nil
}
