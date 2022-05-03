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

	// Much of this package relies on the fact that the Go GC is non-moving.
	// When we move to a new Go version, this dependency should be updated
	// to ensure that the new version is still a non-moving GC.
	_ "go4.org/unsafe/assume-no-moving-gc"
)

// Buffer A generic Buffer object used by some TileDB APIs
type Buffer struct {
	tiledbBuffer *C.tiledb_buffer_t
	context      *Context

	// goBytesSet is a reference to the last Go slice that the buffer was set to.
	//
	// Buffer technically violates the contract of CGo, by passing []byte slices
	// to C code, which holds onto it long after the CGo call has returned.
	// This means that, without keeping this around, Go thinks it can collect
	// the store that we've passed in:
	//
	//     someBytes := getSomeBytes()
	//     buf.SetBuffer(someBytes)
	//     // if it's not referenced later, someBytes might now be collected!
	//
	// By holding onto this reference here, we shield the caller from this
	// happening to them. This is still unsafe per the language spec, but because
	// the Go garbage collector (as of v1.18) does not move objects around,
	// this is not THAT dangerous at runtime.
	goBytesSet []byte
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
	freeOnGC(&buffer)

	return &buffer, nil
}

// Free releases the internal TileDB core data that was allocated on the C heap.
// It is automatically called when this object is garbage collected, but can be
// called earlier to manually release memory if needed. Free is idempotent and
// can safely be called many times on the same object; if it has already
// been freed, it will not be freed again.
func (b *Buffer) Free() {
	b.goBytesSet = nil
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

// Serialize returns a copy of the bytes in the buffer.
func (b *Buffer) Serialize(serializationType SerializationType) ([]byte, error) {
	bs, err := b.dataCopy()
	if err != nil {
		return nil, err
	}
	switch serializationType {
	case TILEDB_CAPNP:
		// The entire byte array contains Cap'nP data. Don't bother it.
	case TILEDB_JSON:
		// The data is a null-terminated string. Strip off the terminator.
		bs = bytes.TrimSuffix(bs, []byte{0})
	default:
		return nil, fmt.Errorf("unsupported serialization type: %v", serializationType)
	}
	return bs, nil
}

// SetBuffer sets the data pointer and size on the Buffer to the given slice
func (b *Buffer) SetBuffer(buffer []byte) error {
	b.goBytesSet = buffer
	bufferSize := len(buffer)

	ret := C.tiledb_buffer_set_data(b.context.tiledbContext, b.tiledbBuffer, unsafe.Pointer(&buffer[0]), C.uint64_t(bufferSize))
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error setting tiledb buffer: %s", b.context.LastError())
	}

	return nil
}

// dataCopy returns a copy of the bytes stored in the buffer.
func (b *Buffer) dataCopy() ([]byte, error) {
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
