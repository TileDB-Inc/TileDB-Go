package tiledb

/*
#cgo LDFLAGS: -ltiledb
#cgo linux LDFLAGS: -ldl
#include <tiledb/tiledb.h>
#include <stdlib.h>
*/
import "C"
import (
	"bytes"
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

	if buffer.context == nil {
		return nil, fmt.Errorf("Error creating tiledb buffer, context is nil")
	}

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

// Free releases the internal TileDB core data that was allocated on the C heap.
// It is automatically called when this object is garbage collected, but can be
// called earlier to manually release memory if needed. Free is idempotent and
// can safely be called many times on the same object; if it has already
// been freed, it will not be freed again.
func (b *Buffer) Free() {
	if b.tiledbBuffer != nil {
		C.tiledb_buffer_free(&b.tiledbBuffer)
	}
}

// Context exposes the internal TileDB context used to initialize the buffer
func (b *Buffer) Context() *Context {
	return b.context
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

func (b *Buffer) Serialize(serializationType SerializationType) ([]byte, error) {
	switch serializationType {
	case TILEDB_CAPNP:
		return b.asCapnp()
	case TILEDB_JSON:
		return b.asJSON()
	default:
		return nil, fmt.Errorf("unsupported serialization type: %v", serializationType)
	}
}

func (b *Buffer) asJSON() ([]byte, error) {
	bs, err := b.bytes()
	if err != nil {
		return nil, err
	}
	// cstrings are null terminated. Go's are not, remove it
	return bytes.TrimSuffix(bs, []byte("\u0000")), nil
}

func (b *Buffer) asCapnp() ([]byte, error) {
	// Create a full copy of the byte slice, as the Buffer object owns the memory.
	bs, err := b.bytes()
	if err != nil {
		return nil, err
	}
	return bs, nil
}

// SetBuffer sets the data pointer and size on the Buffer to the given slice
func (b *Buffer) SetBuffer(buffer []byte) error {
	bufferSize := len(buffer)

	ret := C.tiledb_buffer_set_data(b.context.tiledbContext, b.tiledbBuffer, unsafe.Pointer(&buffer[0]), C.uint64_t(bufferSize))
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error setting tiledb buffer: %s", b.context.LastError())
	}

	return nil
}

// bytes returns a byte slice backed by the underlying C memory region of the buffer
func (b *Buffer) bytes() ([]byte, error) {
	var cbuffer unsafe.Pointer
	var csize C.uint64_t

	ret := C.tiledb_buffer_get_data(b.context.tiledbContext, b.tiledbBuffer, &cbuffer, &csize)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error getting tiledb buffer data: %s", b.context.LastError())
	}

	if cbuffer == nil {
		return nil, nil
	}

	size := uint64(csize)
	bs := (*[1 << 46]uint8)(unsafe.Pointer(cbuffer))[:size:size]

	cpy := make([]byte, len(bs))
	copy(cpy, bs)
	runtime.KeepAlive(b)
	return cpy, nil
}
