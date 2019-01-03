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
	"strconv"
	"unsafe"
)

// KV Creates a key-value store object.
type KV struct {
	tiledbKV *C.tiledb_kv_t
	context  *Context
	uri      string
}

// NewKV alloc a new kv
func NewKV(ctx *Context, uri string) (*KV, error) {
	curi := C.CString(uri)
	defer C.free(unsafe.Pointer(curi))
	kv := KV{context: ctx, uri: uri}
	ret := C.tiledb_kv_alloc(kv.context.tiledbContext, curi, &kv.tiledbKV)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error creating tiledb kv: %s", kv.context.LastError())
	}

	// Set finalizer for free C pointer on gc
	runtime.SetFinalizer(&kv, func(kv *KV) {
		kv.Free()
	})

	return &kv, nil
}

// Free tiledb_kv_t that was allocated on heap in c
func (k *KV) Free() {
	if k.tiledbKV != nil {
		k.Close()
		C.tiledb_kv_free(&k.tiledbKV)
	}
}

// Open the k.  Prepares a key-value store for reading/writing.
func (k *KV) Open(queryType QueryType) error {
	ret := C.tiledb_kv_open(k.context.tiledbContext, k.tiledbKV, C.tiledb_query_type_t(queryType))
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error opening tiledb kv for querying: %s", k.context.LastError())
	}
	return nil
}

/*
OpenWithKey Prepares an encrypted key-value store for reading/writing.

An encrypted key-value store must be opened with this function before queries can be issued to it.
*/
func (k *KV) OpenWithKey(queryType QueryType, encryptionType EncryptionType, key string) error {
	ckey := unsafe.Pointer(C.CString(key))
	defer C.free(ckey)
	ret := C.tiledb_kv_open_with_key(k.context.tiledbContext, k.tiledbKV, C.tiledb_query_type_t(queryType), C.tiledb_encryption_type_t(encryptionType), ckey, C.uint32_t(len(key)))
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error opening tiledb kv with key for querying: %s", k.context.LastError())
	}
	return nil
}

/*
OpenAt Similar to tiledb_kv_open, but this function takes as input a timestamp,
representing time in milliseconds ellapsed since
1970-01-01 00:00:00 +0000 (UTC). Opening the array at a timestamp provides a
view of the array with all writes/updates that happened at or before timestamp
(i.e., excluding those that occurred after timestamp). This function is useful
to ensure consistency at a potential distributed setting, where machines need
to operate on the same view of the array.
*/
func (k *KV) OpenAt(queryType QueryType, timestamp uint64) error {
	ret := C.tiledb_kv_open_at(k.context.tiledbContext, k.tiledbKV, C.tiledb_query_type_t(queryType), C.uint64_t(timestamp))
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error opening tiledb kv at %d for querying: %s", timestamp, k.context.LastError())
	}
	return nil
}

/*
OpenAtWithKey Similar to tiledb_kv_open_with_key, but this function
takes as input a timestamp, representing time in milliseconds ellapsed
since 1970-01-01 00:00:00 +0000 (UTC). Opening the kv at a timestamp
provides a view of the kv with all writes/updates that happened at or
before timestamp (i.e., excluding those that occurred after timestamp).
This function is useful to ensure consistency at a potential distributed
setting, where machines need to operate on the same view of the k.
*/
func (k *KV) OpenAtWithKey(queryType QueryType, encryptionType EncryptionType, key string, timestamp uint64) error {
	ckey := unsafe.Pointer(C.CString(key))
	defer C.free(ckey)
	ret := C.tiledb_kv_open_at_with_key(k.context.tiledbContext, k.tiledbKV, C.tiledb_query_type_t(queryType), C.tiledb_encryption_type_t(encryptionType), ckey, C.uint32_t(len(key)), C.uint64_t(timestamp))
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error opening tiledb kv with key at %d for querying: %s", timestamp, k.context.LastError())
	}
	return nil
}

//IsOpen Checks if the key-value store is open
func (k *KV) IsOpen() (bool, error) {
	var isOpen C.int32_t
	ret := C.tiledb_kv_is_open(k.context.tiledbContext, k.tiledbKV, &isOpen)
	if ret != C.TILEDB_OK {
		return false, fmt.Errorf("Error checking if tiledb KV is open: %s", k.context.LastError())
	}

	return int(isOpen) == 1, nil
}

/*
Reopen the kv Reopens a key-value store. This is useful when there were
updates to the key-value store after it got opened. This function reopens
the key-value store so that it can “see” the new fragments.
*/
func (k *KV) Reopen() error {
	ret := C.tiledb_kv_reopen(k.context.tiledbContext, k.tiledbKV)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error reopening tiledb kv for querying: %s", k.context.LastError())
	}
	return nil
}

/*
ReopenAt the kv Reopens a key-value store at a specific timestamp.
*/
func (k *KV) ReopenAt(timestamp uint64) error {
	ret := C.tiledb_kv_reopen_at(k.context.tiledbContext, k.tiledbKV, C.uint64_t(timestamp))
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error reopening tiledb kv for querying: %s", k.context.LastError())
	}
	return nil
}

//Timestamp Returns the timestamp, representing time in milliseconds ellapsed
//since 1970-01-01 00:00:00 +0000 (UTC), at which the KV was opened. See also
//the documentation of tiledb_kv_open_at.
func (k *KV) Timestamp() (uint64, error) {
	var timestamp C.uint64_t
	ret := C.tiledb_kv_get_timestamp(k.context.tiledbContext, k.tiledbKV, &timestamp)
	if ret != C.TILEDB_OK {
		return 0, fmt.Errorf("Error getting tiledb KV opened timestamp: %s", k.context.LastError())
	}

	return uint64(timestamp), nil
}

// Close a tiledb kv, this is called on garbage collection automatically
// All buffered written items will be flushed to persistent storage.
func (k *KV) Close() error {
	ret := C.tiledb_kv_close(k.context.tiledbContext, k.tiledbKV)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error closing tiledb kv for querying: %s", k.context.LastError())
	}
	return nil
}

// Create a new TileDB kv given an input schemk.
func (k *KV) Create(kvSchema *KVSchema) error {
	curi := C.CString(k.uri)
	defer C.free(unsafe.Pointer(curi))
	ret := C.tiledb_kv_create(k.context.tiledbContext, curi, kvSchema.tiledbKVSchema)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error creating tiledb kv: %s", k.context.LastError())
	}
	return nil
}

// CreateWithKey a new TileDB kv given an input schemk.
func (k *KV) CreateWithKey(kvSchema *KVSchema, encryptionType EncryptionType, key string) error {
	ckey := unsafe.Pointer(C.CString(key))
	defer C.free(ckey)
	curi := C.CString(k.uri)
	defer C.free(unsafe.Pointer(curi))
	ret := C.tiledb_kv_create_with_key(k.context.tiledbContext, curi, kvSchema.tiledbKVSchema, C.tiledb_encryption_type_t(encryptionType), ckey, C.uint32_t(len(key)))
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error creating tiledb kv with key: %s", k.context.LastError())
	}
	return nil
}

// Consolidate Consolidates the fragments of a key-value store into a single fragment.
func (k *KV) Consolidate(config *Config) error {
	if config == nil {
		return fmt.Errorf("Config must not be nil for Consolidate")
	}
	curi := C.CString(k.uri)
	defer C.free(unsafe.Pointer(curi))

	ret := C.tiledb_kv_consolidate(k.context.tiledbContext, curi, config.tiledbConfig)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error consolidating tiledb kv: %s", k.context.LastError())
	}
	return nil
}

// ConsolidateWithKey Consolidates the fragments of an encrypted key-value store into a single fragment.
func (k *KV) ConsolidateWithKey(encryptionType EncryptionType, key string, config *Config) error {
	if config == nil {
		return fmt.Errorf("Config must not be nil for ConsolidateWithKey")
	}
	ckey := unsafe.Pointer(C.CString(key))
	defer C.free(ckey)
	curi := C.CString(k.uri)
	defer C.free(unsafe.Pointer(curi))

	ret := C.tiledb_kv_consolidate_with_key(k.context.tiledbContext, curi, C.tiledb_encryption_type_t(encryptionType), ckey, C.uint32_t(len(key)), config.tiledbConfig)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error consolidating tiledb with key kv: %s", k.context.LastError())
	}
	return nil
}

// Schema returns the KVSchema for the kv
func (k *KV) Schema() (*KVSchema, error) {
	kvSchema := KVSchema{context: k.context}
	ret := C.tiledb_kv_get_schema(k.context.tiledbContext, k.tiledbKV, &kvSchema.tiledbKVSchema)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error getting schema for tiledb kv: %s", k.context.LastError())
	}
	return &kvSchema, nil
}

// AddItem Adds a key-value item to a key-value store. The item is buffered
// internally and periodically flushed to persistent storage. tiledb_kv_flush
// forces flushing the buffered items to storage.
func (k *KV) AddItem(item *KVItem) error {
	ret := C.tiledb_kv_add_item(k.context.tiledbContext, k.tiledbKV, item.tiledbKVItem)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error adding KVItem to tiledb kv: %s", k.context.LastError())
	}
	return nil
}

// Flush Flushes the buffered items to persistent storage.
func (k *KV) Flush() error {
	ret := C.tiledb_kv_flush(k.context.tiledbContext, k.tiledbKV)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error flushing KV: %s", k.context.LastError())
	}
	return nil
}

// Item Retrieves a key-value item based on the input key. If the item with
// the input key does not exist, kv_item is set to NULL.
func (k *KV) Item(key interface{}) (*KVItem, error) {
	kvItem, err := NewKVItem(k.context)
	if err != nil {
		return nil, err
	}

	var ret C.int32_t
	switch t := key.(type) {
	case int:
		i := key.(int)
		if strconv.IntSize == 32 {
			ret = C.tiledb_kv_get_item(k.context.tiledbContext, k.tiledbKV, unsafe.Pointer(&i), C.TILEDB_INT32, C.uint64_t(unsafe.Sizeof(i)), &kvItem.tiledbKVItem)
		} else {
			ret = C.tiledb_kv_get_item(k.context.tiledbContext, k.tiledbKV, unsafe.Pointer(&i), C.TILEDB_INT64, C.uint64_t(unsafe.Sizeof(i)), &kvItem.tiledbKVItem)
		}
	case int8:
		i := key.(int8)
		ret = C.tiledb_kv_get_item(k.context.tiledbContext, k.tiledbKV, unsafe.Pointer(&i), C.TILEDB_INT8, C.uint64_t(unsafe.Sizeof(i)), &kvItem.tiledbKVItem)
	case int16:
		i := key.(int16)
		ret = C.tiledb_kv_get_item(k.context.tiledbContext, k.tiledbKV, unsafe.Pointer(&i), C.TILEDB_INT16, C.uint64_t(unsafe.Sizeof(i)), &kvItem.tiledbKVItem)
	case int32:
		i := key.(int32)
		ret = C.tiledb_kv_get_item(k.context.tiledbContext, k.tiledbKV, unsafe.Pointer(&i), C.TILEDB_INT32, C.uint64_t(unsafe.Sizeof(i)), &kvItem.tiledbKVItem)
	case int64:
		i := key.(int64)
		ret = C.tiledb_kv_get_item(k.context.tiledbContext, k.tiledbKV, unsafe.Pointer(&i), C.TILEDB_INT64, C.uint64_t(unsafe.Sizeof(i)), &kvItem.tiledbKVItem)
	case uint:
		i := key.(uint)
		if strconv.IntSize == 32 {
			ret = C.tiledb_kv_get_item(k.context.tiledbContext, k.tiledbKV, unsafe.Pointer(&i), C.TILEDB_UINT32, C.uint64_t(unsafe.Sizeof(i)), &kvItem.tiledbKVItem)
		} else {
			ret = C.tiledb_kv_get_item(k.context.tiledbContext, k.tiledbKV, unsafe.Pointer(&i), C.TILEDB_UINT64, C.uint64_t(unsafe.Sizeof(i)), &kvItem.tiledbKVItem)
		}
	case uint8:
		i := key.(uint8)
		ret = C.tiledb_kv_get_item(k.context.tiledbContext, k.tiledbKV, unsafe.Pointer(&i), C.TILEDB_UINT8, C.uint64_t(unsafe.Sizeof(i)), &kvItem.tiledbKVItem)
	case uint16:
		i := key.(uint16)
		ret = C.tiledb_kv_get_item(k.context.tiledbContext, k.tiledbKV, unsafe.Pointer(&i), C.TILEDB_UINT16, C.uint64_t(unsafe.Sizeof(i)), &kvItem.tiledbKVItem)
	case uint32:
		i := key.(uint32)
		ret = C.tiledb_kv_get_item(k.context.tiledbContext, k.tiledbKV, unsafe.Pointer(&i), C.TILEDB_UINT32, C.uint64_t(unsafe.Sizeof(i)), &kvItem.tiledbKVItem)
	case uint64:
		i := key.(uint64)
		ret = C.tiledb_kv_get_item(k.context.tiledbContext, k.tiledbKV, unsafe.Pointer(&i), C.TILEDB_UINT64, C.uint64_t(unsafe.Sizeof(i)), &kvItem.tiledbKVItem)
	case float32:
		f := key.(float32)
		ret = C.tiledb_kv_get_item(k.context.tiledbContext, k.tiledbKV, unsafe.Pointer(&f), C.TILEDB_FLOAT32, C.uint64_t(unsafe.Sizeof(f)), &kvItem.tiledbKVItem)
	case float64:
		f := key.(float64)
		ret = C.tiledb_kv_get_item(k.context.tiledbContext, k.tiledbKV, unsafe.Pointer(&f), C.TILEDB_FLOAT64, C.uint64_t(unsafe.Sizeof(f)), &kvItem.tiledbKVItem)
	case string:
		s := key.(string)
		ckey := unsafe.Pointer(C.CString(s))
		defer C.free(ckey)
		ret = C.tiledb_kv_get_item(k.context.tiledbContext, k.tiledbKV, ckey, C.TILEDB_CHAR, C.uint64_t(int(unsafe.Sizeof(s[0]))*len(s)), &kvItem.tiledbKVItem)
	case []int:
		a := key.([]int)
		ckey := unsafe.Pointer(&a[0])
		if strconv.IntSize == 32 {
			ret = C.tiledb_kv_get_item(k.context.tiledbContext, k.tiledbKV, ckey, C.TILEDB_INT32, C.uint64_t(len(a)*int(unsafe.Sizeof(a[0]))), &kvItem.tiledbKVItem)
		} else {
			ret = C.tiledb_kv_get_item(k.context.tiledbContext, k.tiledbKV, ckey, C.TILEDB_INT64, C.uint64_t(len(a)*int(unsafe.Sizeof(a[0]))), &kvItem.tiledbKVItem)
		}
	case []int8:
		a := key.([]int8)
		ckey := unsafe.Pointer(&a[0])
		ret = C.tiledb_kv_get_item(k.context.tiledbContext, k.tiledbKV, ckey, C.TILEDB_INT8, C.uint64_t(len(a)*int(unsafe.Sizeof(a[0]))), &kvItem.tiledbKVItem)
	case []int16:
		a := key.([]int16)
		ckey := unsafe.Pointer(&a[0])
		ret = C.tiledb_kv_get_item(k.context.tiledbContext, k.tiledbKV, ckey, C.TILEDB_INT16, C.uint64_t(len(a)*int(unsafe.Sizeof(a[0]))), &kvItem.tiledbKVItem)
	case []int32:
		a := key.([]int32)
		ckey := unsafe.Pointer(&a[0])
		ret = C.tiledb_kv_get_item(k.context.tiledbContext, k.tiledbKV, ckey, C.TILEDB_INT32, C.uint64_t(len(a)*int(unsafe.Sizeof(a[0]))), &kvItem.tiledbKVItem)
	case []int64:
		a := key.([]int64)
		ckey := unsafe.Pointer(&a[0])
		ret = C.tiledb_kv_get_item(k.context.tiledbContext, k.tiledbKV, ckey, C.TILEDB_INT64, C.uint64_t(len(a)*int(unsafe.Sizeof(a[0]))), &kvItem.tiledbKVItem)
	case []uint:
		a := key.([]uint)
		ckey := unsafe.Pointer(&a[0])
		if strconv.IntSize == 32 {
			ret = C.tiledb_kv_get_item(k.context.tiledbContext, k.tiledbKV, ckey, C.TILEDB_UINT32, C.uint64_t(len(a)*int(unsafe.Sizeof(a[0]))), &kvItem.tiledbKVItem)
		} else {
			ret = C.tiledb_kv_get_item(k.context.tiledbContext, k.tiledbKV, ckey, C.TILEDB_UINT64, C.uint64_t(len(a)*int(unsafe.Sizeof(a[0]))), &kvItem.tiledbKVItem)
		}
	case []uint8:
		a := key.([]uint8)
		ckey := unsafe.Pointer(&a[0])
		ret = C.tiledb_kv_get_item(k.context.tiledbContext, k.tiledbKV, ckey, C.TILEDB_UINT8, C.uint64_t(len(a)*int(unsafe.Sizeof(a[0]))), &kvItem.tiledbKVItem)
	case []uint16:
		a := key.([]uint16)
		ckey := unsafe.Pointer(&a[0])
		ret = C.tiledb_kv_get_item(k.context.tiledbContext, k.tiledbKV, ckey, C.TILEDB_UINT16, C.uint64_t(len(a)*int(unsafe.Sizeof(a[0]))), &kvItem.tiledbKVItem)
	case []uint32:
		a := key.([]uint32)
		ckey := unsafe.Pointer(&a[0])
		ret = C.tiledb_kv_get_item(k.context.tiledbContext, k.tiledbKV, ckey, C.TILEDB_UINT32, C.uint64_t(len(a)*int(unsafe.Sizeof(a[0]))), &kvItem.tiledbKVItem)
	case []uint64:
		a := key.([]uint64)
		ckey := unsafe.Pointer(&a[0])
		ret = C.tiledb_kv_get_item(k.context.tiledbContext, k.tiledbKV, ckey, C.TILEDB_UINT64, C.uint64_t(len(a)*int(unsafe.Sizeof(a[0]))), &kvItem.tiledbKVItem)
	case []float32:
		a := key.([]float32)
		ckey := unsafe.Pointer(&a[0])
		ret = C.tiledb_kv_get_item(k.context.tiledbContext, k.tiledbKV, ckey, C.TILEDB_FLOAT32, C.uint64_t(len(a)*int(unsafe.Sizeof(a[0]))), &kvItem.tiledbKVItem)
	case []float64:
		a := key.([]float64)
		ckey := unsafe.Pointer(&a[0])
		ret = C.tiledb_kv_get_item(k.context.tiledbContext, k.tiledbKV, ckey, C.TILEDB_FLOAT64, C.uint64_t(len(a)*int(unsafe.Sizeof(a[0]))), &kvItem.tiledbKVItem)
	default:
		return nil, fmt.Errorf("Unsupported key type for KV Item(): %T", t)
	}

	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error getting KVItem for key (%s): %s", key, k.context.LastError())
	}
	return kvItem, nil
}

// IsDirty Checks if the key-value store is dirty, i.e., if the user added
// items to the key-value store that are kept in main-memory and have not been
// flushed to persistent storage.
func (k *KV) IsDirty() (bool, error) {
	var isDirty C.int32_t
	ret := C.tiledb_kv_is_dirty(k.context.tiledbContext, k.tiledbKV, &isDirty)
	if ret != C.TILEDB_OK {
		return false, fmt.Errorf("Error getting if KV is dirty: %s", k.context.LastError())
	}
	return int(isDirty) == 1, nil
}

// Iterate of the kv store
// for iter, err := kv.Iterate(); !iter.IsDone(); iter.Next(){
//    kvItem, err := iter.Here()
//	}
func (k *KV) Iterate() (*KVIter, error) {
	return NewKVIter(k)
}
