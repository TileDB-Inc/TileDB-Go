package tiledb

/*
#include <tiledb/tiledb.h>
#include <stdlib.h>
*/
import "C"
import (
	"bytes"
	"fmt"
	"io"
	"math"
	"runtime"
	"unsafe"
)

// Buffer A generic Buffer object used by some TileDB APIs
type Buffer struct {
	tiledbBuffer *C.tiledb_buffer_t
	context      *Context

	// data is a reference to the memory that this Buffer refers to.
	// If this is set to `nil`, the Buffer is was allocated and its memory is
	// owned by TileDB internals.
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
	data byteBuffer
}

// NewBuffer allocates a new buffer.
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
	b.data = nil
	if b.tiledbBuffer != nil {
		C.tiledb_buffer_free(&b.tiledbBuffer)
	}
}

// Context exposes the internal TileDB context used to initialize the buffer.
func (b *Buffer) Context() *Context {
	return b.context
}

// SetType sets the buffer datatype.
func (b *Buffer) SetType(datatype Datatype) error {
	ret := C.tiledb_buffer_set_type(b.context.tiledbContext, b.tiledbBuffer, C.tiledb_datatype_t(datatype))
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error setting datatype for tiledb buffer: %s", b.context.LastError())
	}
	return nil
}

// Type returns the buffer datatype.
func (b *Buffer) Type() (Datatype, error) {
	var bufferType C.tiledb_datatype_t
	ret := C.tiledb_buffer_get_type(b.context.tiledbContext, b.tiledbBuffer, &bufferType)

	if ret != C.TILEDB_OK {
		return 0, fmt.Errorf("Error getting tiledb buffer type: %s", b.context.LastError())
	}

	return Datatype(bufferType), nil
}

// Serialize returns a copy of the bytes in the buffer.
//
// Deprecated: Use WriteTo instead for increased performance.
func (b *Buffer) Serialize(serializationType SerializationType) ([]byte, error) {
	bs, err := b.dataCopy()
	if err != nil {
		return nil, err
	}
	switch serializationType {
	case TILEDB_CAPNP:
		// The entire byte array contains Cap'nP data. Don't bother it.
	case TILEDB_JSON:
		// The data might be a null-terminated string. Strip off the terminator.
		bs = bytes.TrimSuffix(bs, []byte{0})
	default:
		return nil, fmt.Errorf("unsupported serialization type: %v", serializationType)
	}
	return bs, nil
}

// ReadAt writes the contents of a Buffer at a given offset to a slice.
func (b *Buffer) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, fmt.Errorf("offset cannot be negative")
	}

	var cbuffer unsafe.Pointer
	var csize C.uint64_t

	ret := C.tiledb_buffer_get_data(b.context.tiledbContext, b.tiledbBuffer, &cbuffer, &csize)
	if ret != C.TILEDB_OK {
		return 0, fmt.Errorf("error getting tiledb buffer data: %w", b.context.LastError())
	}

	if uintptr(off) > uintptr(csize) {
		return 0, fmt.Errorf("offset cannot be greater than buffer size")
	}

	if cbuffer == nil || csize == 0 {
		return 0, nil
	}

	availableBytes := uint64(csize) - uint64(off)
	var sizeToRead int
	if availableBytes > math.MaxInt {
		sizeToRead = math.MaxInt
	} else {
		sizeToRead = int(availableBytes)
	}

	readSize := copy(p, unsafe.Slice((*byte)(unsafe.Pointer(uintptr(cbuffer)+uintptr(off))), sizeToRead))

	var err error
	if int64(readSize)+off == int64(csize) {
		err = io.EOF
	}

	return readSize, err
}

// WriteTo writes the contents of a Buffer to an io.Writer.
func (b *Buffer) WriteTo(w io.Writer) (int64, error) {
	var cbuffer unsafe.Pointer
	var csize C.uint64_t

	ret := C.tiledb_buffer_get_data(b.context.tiledbContext, b.tiledbBuffer, &cbuffer, &csize)
	if ret != C.TILEDB_OK {
		return 0, fmt.Errorf("error getting tiledb buffer data: %w", b.context.LastError())
	}

	if cbuffer == nil || csize == 0 {
		return 0, nil
	}

	remaining := int64(csize)

	// Because io.Writer supports writing up to 2GB of data at a time, we have to use a loop
	// for the bigger buffers.
	for remaining > 0 {
		// TODO: Use min on Go 1.21+
		var writeSize int
		if remaining > math.MaxInt {
			writeSize = math.MaxInt
		} else {
			writeSize = int(remaining)
		}

		// Construct a slice from the buffer's data without copying it.
		// Keep the buffer alive during the write, to prevent the GC from
		// collecting the memory while it's being used.
		n, err := w.Write(unsafe.Slice((*byte)(cbuffer), writeSize))

		cbuffer = unsafe.Pointer(uintptr(cbuffer) + uintptr(n))
		remaining -= int64(n)

		if err != nil {
			return int64(csize) - remaining, fmt.Errorf("error writing buffer to writer: %w", err)
		}
	}

	return int64(csize), nil
}

// Static assert that Buffer implements io.WriterTo.
var _ io.WriterTo = (*Buffer)(nil)
var _ io.ReaderAt = (*Buffer)(nil)

// SetBuffer sets the buffer to point at the given Go slice. The memory is now
// Go-managed.
func (b *Buffer) SetBuffer(buffer []byte) error {
	b.data = byteBuffer(buffer)

	ret := C.tiledb_buffer_set_data(b.context.tiledbContext, b.tiledbBuffer, b.data.start(), C.uint64_t(b.data.lenBytes()))
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

	if b.data == nil {
		// This is a TileDB-managed buffer. We need to copy its data into Go memory.
		// We assume that once a buffer is set to point to user-provided memory,
		// TileDB never updates the buffer to point to its own memory (i.e., the
		// only time when there will be a buffer pointing to TileDB-owned memory is
		// when TileDB allocates a fresh buffer, e.g. as an out parameter from a
		// serialization function).

		// Since this buffer is TileDB-managed, make sure it's not GC'd before we're
		// done with its memory.
		defer runtime.KeepAlive(b)

		if csize > math.MaxInt32 {
			return nil, fmt.Errorf("TileDB's buffer (%d) larger than maximum allowed CGo buffer (%d)", csize, math.MaxInt32)
		}
		return C.GoBytes(cbuffer, C.int(csize)), nil
	}

	gotBytes := b.data.subSlice(cbuffer, uintptr(csize))

	cpy := make([]byte, len(gotBytes))
	copy(cpy, gotBytes)
	return cpy, nil
}

func (b *Buffer) Len() (uint64, error) {
	var cbuffer unsafe.Pointer
	var csize C.uint64_t

	ret := C.tiledb_buffer_get_data(b.context.tiledbContext, b.tiledbBuffer, &cbuffer, &csize)
	if ret != C.TILEDB_OK {
		return 0, fmt.Errorf("Error getting tiledb buffer data: %s", b.context.LastError())
	}

	return uint64(csize), nil
}
