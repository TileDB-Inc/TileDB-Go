package tiledb

/*
#cgo LDFLAGS: -ltiledb
#include <tiledb/tiledb.h>
#include <stdlib.h>
*/
import "C"

import (
	"fmt"
	"runtime"
)

// KVIter Creates a key-value store iterator object.
type KVIter struct {
	kv           *KV
	tiledbKVIter *C.tiledb_kv_iter_t
	context      *Context
}

// NewKVIter Creates an iterator for a key-value store. This can be used
// only for reading. This sets the pointer to the first key-value item.
func NewKVIter(kv *KV) (*KVIter, error) {
	k := KVIter{context: kv.context, kv: kv}
	ret := C.tiledb_kv_iter_alloc(k.context.tiledbContext, kv.tiledbKV, &k.tiledbKVIter)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error creating tiledb kv iter: %s", k.context.LastError())
	}

	// Set finalizer for free C pointer on gc
	runtime.SetFinalizer(&k, func(k *KVIter) {
		k.Free()
	})

	return &k, nil
}

// Free tiledb_kv_t that was allocated on heap in c
func (k *KVIter) Free() {
	if k.tiledbKVIter != nil {
		C.tiledb_kv_iter_free(&k.tiledbKVIter)
	}
}

// Here Retrieves the key-value item currently pointed by the iterator.
// Note that this function creates a new key-value item.
func (k *KVIter) Here() (*KVItem, error) {
	kvItem, err := NewKVItem(k.context)
	if err != nil {
		return nil, err
	}
	kvItem.Free()

	ret := C.tiledb_kv_iter_here(k.context.tiledbContext, k.tiledbKVIter, &kvItem.tiledbKVItem)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error getting KVItem from iter: %s", k.context.LastError().Error())
	}

	return kvItem, nil
}

// Next Moves the iterator to the next item.
func (k *KVIter) Next() error {
	ret := C.tiledb_kv_iter_next(k.context.tiledbContext, k.tiledbKVIter)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error moving to next KVItem from iter: %s", k.context.LastError().Error())
	}
	return nil
}

// Done Checks if the iterator is done.
func (k *KVIter) Done() (bool, error) {
	var cDone C.int32_t
	ret := C.tiledb_kv_iter_done(k.context.tiledbContext, k.tiledbKVIter, &cDone)
	if ret != C.TILEDB_OK {
		return false, fmt.Errorf("Error moving to next KVItem from iter: %s", k.context.LastError().Error())
	}
	return int(cDone) == 1, nil
}

// IsDone Checks if the iterator is done.
func (k *KVIter) IsDone() bool {
	var cDone C.int32_t
	ret := C.tiledb_kv_iter_done(k.context.tiledbContext, k.tiledbKVIter, &cDone)
	if ret != C.TILEDB_OK {
		return false
	}
	return int(cDone) == 1
}

// Reset a key-value store iterator.
func (k *KVIter) Reset() error {
	ret := C.tiledb_kv_iter_reset(k.context.tiledbContext, k.tiledbKVIter)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error resetting KVIter: %s", k.context.LastError().Error())
	}
	return nil
}
