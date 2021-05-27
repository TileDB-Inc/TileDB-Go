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
)

// BufferList A list of TileDB BufferList objects
type BufferList struct {
	tiledbBufferList *C.tiledb_buffer_list_t
	context          *Context
}

// NewBufferList Allocs a new buffer list
func NewBufferList(context *Context) (*BufferList, error) {
	bufferList := BufferList{context: context}

	ret := C.tiledb_buffer_list_alloc(bufferList.context.tiledbContext, &bufferList.tiledbBufferList)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error creating tiledb buffer list: %s", bufferList.context.LastError())
	}

	// Set finalizer for free C pointer on gc
	runtime.SetFinalizer(&bufferList, func(bufferList *BufferList) {
		bufferList.Free()
	})

	return &bufferList, nil
}

// Free c-alloc'ed data types
func (b *BufferList) Free() {
	if b.tiledbBufferList != nil {
		C.tiledb_buffer_list_free(&b.tiledbBufferList)
	}
}

// NumBuffers returns number of buffers in the list
func (b *BufferList) NumBuffers() (uint64, error) {
	var numBuffers C.uint64_t
	ret := C.tiledb_buffer_list_get_num_buffers(b.context.tiledbContext, b.tiledbBufferList, &numBuffers)

	if ret != C.TILEDB_OK {
		return 0, fmt.Errorf("Error getting tiledb bufferList num buffers: %s", b.context.LastError())
	}

	return uint64(numBuffers), nil
}

// GetBuffer returns a Buffer at the given index in the list
func (b *BufferList) GetBuffer(bufferIndex uint) (*Buffer, error) {
	buffer := Buffer{context: b.context}
	// Set finalizer for free C pointer on gc
	runtime.SetFinalizer(&buffer, func(buffer *Buffer) {
		buffer.Free()
	})

	ret := C.tiledb_buffer_list_get_buffer(b.context.tiledbContext, b.tiledbBufferList, C.uint64_t(bufferIndex), &buffer.tiledbBuffer)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error getting tiledb buffer index %d from buffer list: %s", bufferIndex, b.context.LastError())
	}

	return &buffer, nil
}

// TotalSize returns total number of bytes in the buffers in the list
func (b *BufferList) TotalSize() (uint64, error) {
	var totalSize C.uint64_t
	ret := C.tiledb_buffer_list_get_total_size(b.context.tiledbContext, b.tiledbBufferList, &totalSize)

	if ret != C.TILEDB_OK {
		return 0, fmt.Errorf("Error getting tiledb bufferList num buffers: %s", b.context.LastError())
	}

	return uint64(totalSize), nil
}

// Flatten copies and concatenates all buffers in the list into a new buffer
func (b *BufferList) Flatten() (*Buffer, error) {
	buffer := Buffer{context: b.context}
	// Set finalizer for free C pointer on gc
	runtime.SetFinalizer(&buffer, func(buffer *Buffer) {
		buffer.Free()
	})

	ret := C.tiledb_buffer_list_flatten(b.context.tiledbContext, b.tiledbBufferList, &buffer.tiledbBuffer)

	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error getting tiledb bufferList num buffers: %s", b.context.LastError())
	}

	return &buffer, nil
}
