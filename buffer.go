package tiledb

/*
#include <tiledb/tiledb.h>
#include <stdlib.h>
*/
import "C"
import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"runtime"
	"unsafe"
)

// bufferHandleState contains a native TileDB buffer handle, and the resources that must be released
// alongside it.
// Cleanup-based finalizers do not run in a predetermined order, so this type exists to tie the lifetime
// of a buffer and its pinner together.
type bufferHandleState struct {
	ptr    *C.tiledb_buffer_t
	pinner runtime.Pinner
}

func freeCapiBufferState(p unsafe.Pointer) {
	h := (*bufferHandleState)(p)
	if h.ptr != nil {
		C.tiledb_buffer_free(&h.ptr)
	}
	h.pinner.Unpin()
}

type bufferHandle struct {
	// Important: this capiHandle stores a pointer to bufferHandleState, not to tiledb_buffer_t!
	*capiHandle
}

func newBufferHandle(ptr *C.tiledb_buffer_t) bufferHandle {
	state := &bufferHandleState{ptr: ptr}
	return bufferHandle{newCapiHandle(unsafe.Pointer(state), freeCapiBufferState)}
}

func (x bufferHandle) getState() *bufferHandleState {
	return (*bufferHandleState)(x.capiHandle.Get())
}

func (x bufferHandle) Get() *C.tiledb_buffer_t {
	return x.getState().ptr
}

func (x bufferHandle) Pin(p any) {
	x.getState().pinner.Pin(p)
}

// Buffer A generic Buffer object used by some TileDB APIs
type Buffer struct {
	tiledbBuffer bufferHandle
	context      *Context
	pinner       runtime.Pinner
}

func newBufferFromHandle(context *Context, handle bufferHandle) *Buffer {
	return &Buffer{tiledbBuffer: handle, context: context}
}

// NewBuffer allocates a new buffer.
func NewBuffer(context *Context) (*Buffer, error) {
	if context == nil {
		return nil, errors.New("error creating tiledb buffer, context is nil")
	}

	var bufferPtr *C.tiledb_buffer_t
	ret := C.tiledb_buffer_alloc(context.tiledbContext, &bufferPtr)
	runtime.KeepAlive(context)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("error creating tiledb buffer: %w", context.LastError())
	}

	return newBufferFromHandle(context, newBufferHandle(bufferPtr)), nil
}

// Free releases the internal TileDB core data that was allocated on the C heap.
// It is automatically called when this object is garbage collected, but can be
// called earlier to manually release memory if needed. Free is idempotent and
// can safely be called many times on the same object; if it has already
// been freed, it will not be freed again.
func (b *Buffer) Free() {
	b.tiledbBuffer.Free()
}

// Context exposes the internal TileDB context used to initialize the buffer.
func (b *Buffer) Context() *Context {
	return b.context
}

// SetType sets the buffer datatype.
func (b *Buffer) SetType(datatype Datatype) error {
	ret := C.tiledb_buffer_set_type(b.context.tiledbContext, b.tiledbBuffer.Get(), C.tiledb_datatype_t(datatype))
	runtime.KeepAlive(b)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("error setting datatype for tiledb buffer: %w", b.context.LastError())
	}
	return nil
}

// Type returns the buffer datatype.
func (b *Buffer) Type() (Datatype, error) {
	var bufferType C.tiledb_datatype_t
	ret := C.tiledb_buffer_get_type(b.context.tiledbContext, b.tiledbBuffer.Get(), &bufferType)
	runtime.KeepAlive(b)

	if ret != C.TILEDB_OK {
		return 0, fmt.Errorf("error getting tiledb buffer type: %w", b.context.LastError())
	}

	return Datatype(bufferType), nil
}

// Serialize returns a copy of the bytes in the buffer.
//
// Deprecated: Use WriteTo or ReadAt instead for increased performance.
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
		return 0, errors.New("offset cannot be negative")
	}

	var cbuffer unsafe.Pointer // b must be kept alive while cbuffer is being accessed.
	var csize C.uint64_t

	ret := C.tiledb_buffer_get_data(b.context.tiledbContext, b.tiledbBuffer.Get(), &cbuffer, &csize)
	if ret != C.TILEDB_OK {
		return 0, fmt.Errorf("error getting tiledb buffer data: %w", b.context.LastError())
	}

	if uintptr(off) >= uintptr(csize) || cbuffer == nil {
		// Match ReaderAt behavior of os.File and fail with io.EOF if the offset is greater or equal to the size.
		return 0, io.EOF
	}

	availableBytes := uint64(csize) - uint64(off)
	sizeToRead := min(math.MaxInt, int(availableBytes))

	readSize := copy(p, unsafe.Slice((*byte)(unsafe.Pointer(uintptr(cbuffer)+uintptr(off))), sizeToRead))
	runtime.KeepAlive(b)

	var err error
	if int64(readSize)+off == int64(csize) {
		err = io.EOF
	}

	return readSize, err
}

// WriteTo writes the contents of a Buffer to an io.Writer.
func (b *Buffer) WriteTo(w io.Writer) (int64, error) {
	var cbuffer unsafe.Pointer // b must be kept alive while cbuffer is being accessed.
	var csize C.uint64_t

	ret := C.tiledb_buffer_get_data(b.context.tiledbContext, b.tiledbBuffer.Get(), &cbuffer, &csize)
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
		writeSize := min(math.MaxInt, int(remaining))

		// Construct a slice from the buffer's data without copying it.
		n, err := w.Write(unsafe.Slice((*byte)(unsafe.Pointer(uintptr(cbuffer)+uintptr(csize)-uintptr(remaining))), writeSize))
		runtime.KeepAlive(b)
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
	cbuffer := unsafe.Pointer(unsafe.SliceData(buffer))
	b.tiledbBuffer.Pin(cbuffer)

	ret := C.tiledb_buffer_set_data(b.context.tiledbContext, b.tiledbBuffer.Get(), cbuffer, C.uint64_t(len(buffer)))
	runtime.KeepAlive(b)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("error setting tiledb buffer: %w", b.context.LastError())
	}

	return nil
}

// dataCopy returns a copy of the bytes stored in the buffer.
func (b *Buffer) dataCopy() ([]byte, error) {
	var cbuffer unsafe.Pointer // b must be kept alive while cbuffer is being accessed.
	var csize C.uint64_t

	ret := C.tiledb_buffer_get_data(b.context.tiledbContext, b.tiledbBuffer.Get(), &cbuffer, &csize)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("error getting tiledb buffer data: %w", b.context.LastError())
	}

	if cbuffer == nil {
		return nil, nil
	}

	if csize > math.MaxInt32 {
		return nil, fmt.Errorf("TileDB's buffer (%d) larger than maximum allowed CGo buffer (%d)", csize, math.MaxInt32)
	}
	cpy := C.GoBytes(cbuffer, C.int(csize))

	runtime.KeepAlive(b)
	return cpy, nil
}

func (b *Buffer) Len() (uint64, error) {
	var cbuffer unsafe.Pointer
	var csize C.uint64_t

	ret := C.tiledb_buffer_get_data(b.context.tiledbContext, b.tiledbBuffer.Get(), &cbuffer, &csize)
	runtime.KeepAlive(b)
	if ret != C.TILEDB_OK {
		return 0, fmt.Errorf("error getting tiledb buffer data: %w", b.context.LastError())
	}

	return uint64(csize), nil
}
