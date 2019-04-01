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
	"strconv"
	"unsafe"
)

// KV Creates a key-value store object.
type KVItem struct {
	tiledbKVItem *C.tiledb_kv_item_t
	context      *Context
}

// NewKV alloc a new kv
func NewKVItem(ctx *Context) (*KVItem, error) {
	kvItem := KVItem{context: ctx}
	ret := C.tiledb_kv_item_alloc(kvItem.context.tiledbContext, &kvItem.tiledbKVItem)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error creating tiledb kv: %s", kvItem.context.LastError())
	}

	// Set finalizer for free C pointer on gc
	runtime.SetFinalizer(&kvItem, func(kvItem *KVItem) {
		kvItem.Free()
	})

	return &kvItem, nil
}

// Free tiledb_kv_t that was allocated on heap in c
func (k *KVItem) Free() {
	if k.tiledbKVItem != nil {
		C.tiledb_kv_item_free(&k.tiledbKVItem)
	}
}

func (k *KVItem) SetKey(key interface{}) error {
	var ret C.int32_t
	switch t := key.(type) {
	case int:
		i := key.(int)
		if strconv.IntSize == 32 {
			ret = C.tiledb_kv_item_set_key(k.context.tiledbContext, k.tiledbKVItem, unsafe.Pointer(&i), C.TILEDB_INT32, C.uint64_t(unsafe.Sizeof(i)))
		} else {
			ret = C.tiledb_kv_item_set_key(k.context.tiledbContext, k.tiledbKVItem, unsafe.Pointer(&i), C.TILEDB_INT64, C.uint64_t(unsafe.Sizeof(i)))
		}
	case int8:
		i := key.(int8)
		ret = C.tiledb_kv_item_set_key(k.context.tiledbContext, k.tiledbKVItem, unsafe.Pointer(&i), C.TILEDB_INT8, C.uint64_t(unsafe.Sizeof(i)))
	case int16:
		i := key.(int16)
		ret = C.tiledb_kv_item_set_key(k.context.tiledbContext, k.tiledbKVItem, unsafe.Pointer(&i), C.TILEDB_INT16, C.uint64_t(unsafe.Sizeof(i)))
	case int32:
		i := key.(int32)
		ret = C.tiledb_kv_item_set_key(k.context.tiledbContext, k.tiledbKVItem, unsafe.Pointer(&i), C.TILEDB_INT32, C.uint64_t(unsafe.Sizeof(i)))
	case int64:
		i := key.(int64)
		ret = C.tiledb_kv_item_set_key(k.context.tiledbContext, k.tiledbKVItem, unsafe.Pointer(&i), C.TILEDB_INT64, C.uint64_t(unsafe.Sizeof(i)))
	case uint:
		i := key.(uint)
		if strconv.IntSize == 32 {
			ret = C.tiledb_kv_item_set_key(k.context.tiledbContext, k.tiledbKVItem, unsafe.Pointer(&i), C.TILEDB_UINT32, C.uint64_t(unsafe.Sizeof(i)))
		} else {
			ret = C.tiledb_kv_item_set_key(k.context.tiledbContext, k.tiledbKVItem, unsafe.Pointer(&i), C.TILEDB_UINT64, C.uint64_t(unsafe.Sizeof(i)))
		}
	case uint8:
		i := key.(uint8)
		ret = C.tiledb_kv_item_set_key(k.context.tiledbContext, k.tiledbKVItem, unsafe.Pointer(&i), C.TILEDB_UINT8, C.uint64_t(unsafe.Sizeof(i)))
	case uint16:
		i := key.(uint16)
		ret = C.tiledb_kv_item_set_key(k.context.tiledbContext, k.tiledbKVItem, unsafe.Pointer(&i), C.TILEDB_UINT16, C.uint64_t(unsafe.Sizeof(i)))
	case uint32:
		i := key.(uint32)
		ret = C.tiledb_kv_item_set_key(k.context.tiledbContext, k.tiledbKVItem, unsafe.Pointer(&i), C.TILEDB_UINT32, C.uint64_t(unsafe.Sizeof(i)))
	case uint64:
		i := key.(uint64)
		ret = C.tiledb_kv_item_set_key(k.context.tiledbContext, k.tiledbKVItem, unsafe.Pointer(&i), C.TILEDB_UINT64, C.uint64_t(unsafe.Sizeof(i)))
	case float32:
		f := key.(float32)
		ret = C.tiledb_kv_item_set_key(k.context.tiledbContext, k.tiledbKVItem, unsafe.Pointer(&f), C.TILEDB_FLOAT32, C.uint64_t(unsafe.Sizeof(f)))
	case float64:
		f := key.(float64)
		ret = C.tiledb_kv_item_set_key(k.context.tiledbContext, k.tiledbKVItem, unsafe.Pointer(&f), C.TILEDB_FLOAT64, C.uint64_t(unsafe.Sizeof(f)))
	case string:
		s := key.(string)
		ckey := unsafe.Pointer(C.CString(s))
		defer C.free(ckey)
		ret = C.tiledb_kv_item_set_key(k.context.tiledbContext, k.tiledbKVItem, ckey, C.TILEDB_CHAR, C.uint64_t(int(unsafe.Sizeof(s[0]))*len(s)))
	case []int:
		a := key.([]int)
		ckey := unsafe.Pointer(&a[0])
		if strconv.IntSize == 32 {
			ret = C.tiledb_kv_item_set_key(k.context.tiledbContext, k.tiledbKVItem, ckey, C.TILEDB_INT32, C.uint64_t(len(a)*int(unsafe.Sizeof(a[0]))))
		} else {
			ret = C.tiledb_kv_item_set_key(k.context.tiledbContext, k.tiledbKVItem, ckey, C.TILEDB_INT64, C.uint64_t(len(a)*int(unsafe.Sizeof(a[0]))))
		}
	case []int8:
		a := key.([]int8)
		ckey := unsafe.Pointer(&a[0])
		ret = C.tiledb_kv_item_set_key(k.context.tiledbContext, k.tiledbKVItem, ckey, C.TILEDB_INT8, C.uint64_t(len(a)*int(unsafe.Sizeof(a[0]))))
	case []int16:
		a := key.([]int16)
		ckey := unsafe.Pointer(&a[0])
		ret = C.tiledb_kv_item_set_key(k.context.tiledbContext, k.tiledbKVItem, ckey, C.TILEDB_INT16, C.uint64_t(len(a)*int(unsafe.Sizeof(a[0]))))
	case []int32:
		a := key.([]int32)
		ckey := unsafe.Pointer(&a[0])
		ret = C.tiledb_kv_item_set_key(k.context.tiledbContext, k.tiledbKVItem, ckey, C.TILEDB_INT32, C.uint64_t(len(a)*int(unsafe.Sizeof(a[0]))))
	case []int64:
		a := key.([]int64)
		ckey := unsafe.Pointer(&a[0])
		ret = C.tiledb_kv_item_set_key(k.context.tiledbContext, k.tiledbKVItem, ckey, C.TILEDB_INT64, C.uint64_t(len(a)*int(unsafe.Sizeof(a[0]))))
	case []uint:
		a := key.([]uint)
		ckey := unsafe.Pointer(&a[0])
		if strconv.IntSize == 32 {
			ret = C.tiledb_kv_item_set_key(k.context.tiledbContext, k.tiledbKVItem, ckey, C.TILEDB_UINT32, C.uint64_t(len(a)*int(unsafe.Sizeof(a[0]))))
		} else {
			ret = C.tiledb_kv_item_set_key(k.context.tiledbContext, k.tiledbKVItem, ckey, C.TILEDB_UINT64, C.uint64_t(len(a)*int(unsafe.Sizeof(a[0]))))
		}
	case []uint8:
		a := key.([]uint8)
		ckey := unsafe.Pointer(&a[0])
		ret = C.tiledb_kv_item_set_key(k.context.tiledbContext, k.tiledbKVItem, ckey, C.TILEDB_UINT8, C.uint64_t(len(a)*int(unsafe.Sizeof(a[0]))))
	case []uint16:
		a := key.([]uint16)
		ckey := unsafe.Pointer(&a[0])
		ret = C.tiledb_kv_item_set_key(k.context.tiledbContext, k.tiledbKVItem, ckey, C.TILEDB_UINT16, C.uint64_t(len(a)*int(unsafe.Sizeof(a[0]))))
	case []uint32:
		a := key.([]uint32)
		ckey := unsafe.Pointer(&a[0])
		ret = C.tiledb_kv_item_set_key(k.context.tiledbContext, k.tiledbKVItem, ckey, C.TILEDB_UINT32, C.uint64_t(len(a)*int(unsafe.Sizeof(a[0]))))
	case []uint64:
		a := key.([]uint64)
		ckey := unsafe.Pointer(&a[0])
		ret = C.tiledb_kv_item_set_key(k.context.tiledbContext, k.tiledbKVItem, ckey, C.TILEDB_UINT64, C.uint64_t(len(a)*int(unsafe.Sizeof(a[0]))))
	case []float32:
		a := key.([]float32)
		ckey := unsafe.Pointer(&a[0])
		ret = C.tiledb_kv_item_set_key(k.context.tiledbContext, k.tiledbKVItem, ckey, C.TILEDB_FLOAT32, C.uint64_t(len(a)*int(unsafe.Sizeof(a[0]))))
	case []float64:
		a := key.([]float64)
		ckey := unsafe.Pointer(&a[0])
		ret = C.tiledb_kv_item_set_key(k.context.tiledbContext, k.tiledbKVItem, ckey, C.TILEDB_FLOAT64, C.uint64_t(len(a)*int(unsafe.Sizeof(a[0]))))
	default:
		return fmt.Errorf("Unsupported key type for KVItem: %T", t)
	}

	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error setting key for KVItem: %s", k.context.LastError())
	}
	return nil
}

// Key Gets the key in the key-value item.
// This copies the value from a c pointer into a golang type
func (k *KVItem) Key() (interface{}, error) {
	var cKey unsafe.Pointer
	var keySize C.uint64_t
	var keyType C.tiledb_datatype_t
	ret := C.tiledb_kv_item_get_key(k.context.tiledbContext, k.tiledbKVItem, &cKey, &keyType, &keySize)

	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error getting key for KVItem: %s", k.context.LastError())
	}

	switch Datatype(keyType) {
	case TILEDB_INT8:
		// If the key size is greater than the size of a single value in bytes it is an array
		elements := int(keySize) / C.sizeof_int8_t
		if elements > 1 {
			tmpslice := (*[1 << 30]C.int8_t)(unsafe.Pointer(cKey))[:elements:elements]
			retSlice := make([]int8, elements)
			for i, s := range tmpslice {
				retSlice[i] = int8(s)
			}
			return retSlice, nil
		} else {
			return int8(*(*C.int8_t)(cKey)), nil
		}
	case TILEDB_INT16:
		// If the key size is greater than the size of a single value in bytes it is an array
		elements := int(keySize) / C.sizeof_int16_t
		if elements > 1 {
			tmpslice := (*[1 << 30]C.int16_t)(unsafe.Pointer(cKey))[:elements:elements]
			retSlice := make([]int16, elements)
			for i, s := range tmpslice {
				retSlice[i] = int16(s)
			}
			return retSlice, nil
		} else {
			return int16(*(*C.int16_t)(cKey)), nil
		}
	case TILEDB_INT32:
		// If the key size is greater than the size of a single value in bytes it is an array
		elements := int(keySize) / C.sizeof_int32_t
		if elements > 1 {
			tmpslice := (*[1 << 30]C.int32_t)(unsafe.Pointer(cKey))[:elements:elements]
			retSlice := make([]int32, elements)
			for i, s := range tmpslice {
				retSlice[i] = int32(s)
			}
			return retSlice, nil
		} else {
			return int32(*(*C.int32_t)(cKey)), nil
		}
	case TILEDB_INT64:
		// If the key size is greater than the size of a single value in bytes it is an array
		elements := int(keySize) / C.sizeof_int64_t
		if elements > 1 {
			tmpslice := (*[1 << 30]C.int64_t)(unsafe.Pointer(cKey))[:elements:elements]
			retSlice := make([]int64, elements)
			for i, s := range tmpslice {
				retSlice[i] = int64(s)
			}
			return retSlice, nil
		} else {
			return int64(*(*C.int64_t)(cKey)), nil
		}
	case TILEDB_UINT8:
		// If the key size is greater than the size of a single value in bytes it is an array
		elements := int(keySize) / C.sizeof_uint8_t
		if elements > 1 {
			tmpslice := (*[1 << 30]C.uint8_t)(unsafe.Pointer(cKey))[:elements:elements]
			retSlice := make([]uint8, elements)
			for i, s := range tmpslice {
				retSlice[i] = uint8(s)
			}
			return retSlice, nil
		} else {
			return int8(*(*C.uint8_t)(cKey)), nil
		}
	case TILEDB_UINT16:
		// If the key size is greater than the size of a single value in bytes it is an array
		elements := int(keySize) / C.sizeof_uint16_t
		if elements > 1 {
			tmpslice := (*[1 << 30]C.uint16_t)(unsafe.Pointer(cKey))[:elements:elements]
			retSlice := make([]uint16, elements)
			for i, s := range tmpslice {
				retSlice[i] = uint16(s)
			}
			return retSlice, nil
		} else {
			return int16(*(*C.uint16_t)(cKey)), nil
		}
	case TILEDB_UINT32:
		// If the key size is greater than the size of a single value in bytes it is an array
		elements := int(keySize) / C.sizeof_uint32_t
		if elements > 1 {
			tmpslice := (*[1 << 30]C.uint32_t)(unsafe.Pointer(cKey))[:elements:elements]
			retSlice := make([]uint32, elements)
			for i, s := range tmpslice {
				retSlice[i] = uint32(s)
			}
			return retSlice, nil
		} else {
			return int32(*(*C.uint32_t)(cKey)), nil
		}
	case TILEDB_UINT64:
		// If the key size is greater than the size of a single value in bytes it is an array
		elements := int(keySize) / C.sizeof_uint64_t
		if elements > 1 {
			tmpslice := (*[1 << 30]C.uint64_t)(unsafe.Pointer(cKey))[:elements:elements]
			retSlice := make([]uint64, elements)
			for i, s := range tmpslice {
				retSlice[i] = uint64(s)
			}
			return retSlice, nil
		} else {
			return int64(*(*C.uint64_t)(cKey)), nil
		}
	case TILEDB_FLOAT32:
		// If the key size is greater than the size of a single value in bytes it is an array
		elements := int(keySize) / C.sizeof_float
		if elements > 1 {
			tmpslice := (*[1 << 30]C.float)(unsafe.Pointer(cKey))[:elements:elements]
			retSlice := make([]float32, elements)
			for i, s := range tmpslice {
				retSlice[i] = float32(s)
			}
			return retSlice, nil
		} else {
			return float32(*(*C.float)(cKey)), nil
		}
	case TILEDB_FLOAT64:
		// If the key size is greater than the size of a single value in bytes it is an array
		elements := int(keySize) / C.sizeof_double
		if elements > 1 {
			tmpslice := (*[1 << 30]C.double)(unsafe.Pointer(cKey))[:elements:elements]
			retSlice := make([]float64, elements)
			for i, s := range tmpslice {
				retSlice[i] = float64(s)
			}
			return retSlice, nil
		} else {
			return float64(*(*C.double)(cKey)), nil
		}
	case TILEDB_CHAR:
		elements := int(keySize) / C.sizeof_char
		return C.GoStringN((*C.char)(cKey), C.int32_t(elements)), nil

	default:
		return nil, fmt.Errorf("Unsupported tiledb key type: %v", keyType)
	}

	return nil, fmt.Errorf("Error getting key for KVItem")
}

// SetValue Sets a value for a particular attribute to the key-value item. This function works for both fixed- and variable-sized attributes.
func (k *KVItem) SetValue(attribute string, value interface{}) error {
	cAttribute := C.CString(attribute)
	defer C.free(unsafe.Pointer(cAttribute))
	var ret C.int32_t
	switch t := value.(type) {
	case int:
		i := value.(int)
		if strconv.IntSize == 32 {
			ret = C.tiledb_kv_item_set_value(k.context.tiledbContext, k.tiledbKVItem, cAttribute, unsafe.Pointer(&i), C.TILEDB_INT32, C.uint64_t(unsafe.Sizeof(i)))
		} else {
			ret = C.tiledb_kv_item_set_value(k.context.tiledbContext, k.tiledbKVItem, cAttribute, unsafe.Pointer(&i), C.TILEDB_INT64, C.uint64_t(unsafe.Sizeof(i)))
		}
	case int8:
		i := value.(int8)
		ret = C.tiledb_kv_item_set_value(k.context.tiledbContext, k.tiledbKVItem, cAttribute, unsafe.Pointer(&i), C.TILEDB_INT8, C.uint64_t(unsafe.Sizeof(i)))
	case int16:
		i := value.(int16)
		ret = C.tiledb_kv_item_set_value(k.context.tiledbContext, k.tiledbKVItem, cAttribute, unsafe.Pointer(&i), C.TILEDB_INT16, C.uint64_t(unsafe.Sizeof(i)))
	case int32:
		i := value.(int32)
		ret = C.tiledb_kv_item_set_value(k.context.tiledbContext, k.tiledbKVItem, cAttribute, unsafe.Pointer(&i), C.TILEDB_INT32, C.uint64_t(unsafe.Sizeof(i)))
	case int64:
		i := value.(int64)
		ret = C.tiledb_kv_item_set_value(k.context.tiledbContext, k.tiledbKVItem, cAttribute, unsafe.Pointer(&i), C.TILEDB_INT64, C.uint64_t(unsafe.Sizeof(i)))
	case uint:
		i := value.(uint)
		if strconv.IntSize == 32 {
			ret = C.tiledb_kv_item_set_value(k.context.tiledbContext, k.tiledbKVItem, cAttribute, unsafe.Pointer(&i), C.TILEDB_UINT32, C.uint64_t(unsafe.Sizeof(i)))
		} else {
			ret = C.tiledb_kv_item_set_value(k.context.tiledbContext, k.tiledbKVItem, cAttribute, unsafe.Pointer(&i), C.TILEDB_UINT64, C.uint64_t(unsafe.Sizeof(i)))
		}
	case uint8:
		i := value.(uint8)
		ret = C.tiledb_kv_item_set_value(k.context.tiledbContext, k.tiledbKVItem, cAttribute, unsafe.Pointer(&i), C.TILEDB_UINT8, C.uint64_t(unsafe.Sizeof(i)))
	case uint16:
		i := value.(uint16)
		ret = C.tiledb_kv_item_set_value(k.context.tiledbContext, k.tiledbKVItem, cAttribute, unsafe.Pointer(&i), C.TILEDB_UINT16, C.uint64_t(unsafe.Sizeof(i)))
	case uint32:
		i := value.(uint32)
		ret = C.tiledb_kv_item_set_value(k.context.tiledbContext, k.tiledbKVItem, cAttribute, unsafe.Pointer(&i), C.TILEDB_UINT32, C.uint64_t(unsafe.Sizeof(i)))
	case uint64:
		i := value.(uint64)
		ret = C.tiledb_kv_item_set_value(k.context.tiledbContext, k.tiledbKVItem, cAttribute, unsafe.Pointer(&i), C.TILEDB_UINT64, C.uint64_t(unsafe.Sizeof(i)))
	case float32:
		f := value.(float32)
		ret = C.tiledb_kv_item_set_value(k.context.tiledbContext, k.tiledbKVItem, cAttribute, unsafe.Pointer(&f), C.TILEDB_FLOAT32, C.uint64_t(unsafe.Sizeof(f)))
	case float64:
		f := value.(float64)
		ret = C.tiledb_kv_item_set_value(k.context.tiledbContext, k.tiledbKVItem, cAttribute, unsafe.Pointer(&f), C.TILEDB_FLOAT64, C.uint64_t(unsafe.Sizeof(f)))
	case string:
		s := value.(string)
		cvalue := unsafe.Pointer(C.CString(s))
		defer C.free(cvalue)
		ret = C.tiledb_kv_item_set_value(k.context.tiledbContext, k.tiledbKVItem, cAttribute, cvalue, C.TILEDB_CHAR, C.uint64_t(C.sizeof_char*len(s)))
	case []int:
		a := value.([]int)
		cvalue := unsafe.Pointer(&a[0])
		if strconv.IntSize == 32 {
			ret = C.tiledb_kv_item_set_value(k.context.tiledbContext, k.tiledbKVItem, cAttribute, cvalue, C.TILEDB_INT32, C.uint64_t(len(a)*int(unsafe.Sizeof(a[0]))))
		} else {
			ret = C.tiledb_kv_item_set_value(k.context.tiledbContext, k.tiledbKVItem, cAttribute, cvalue, C.TILEDB_INT64, C.uint64_t(len(a)*int(unsafe.Sizeof(a[0]))))
		}
	case []int8:
		a := value.([]int8)
		cvalue := unsafe.Pointer(&a[0])
		ret = C.tiledb_kv_item_set_value(k.context.tiledbContext, k.tiledbKVItem, cAttribute, cvalue, C.TILEDB_INT8, C.uint64_t(len(a)*int(unsafe.Sizeof(a[0]))))
	case []int16:
		a := value.([]int16)
		cvalue := unsafe.Pointer(&a[0])
		ret = C.tiledb_kv_item_set_value(k.context.tiledbContext, k.tiledbKVItem, cAttribute, cvalue, C.TILEDB_INT16, C.uint64_t(len(a)*int(unsafe.Sizeof(a[0]))))
	case []int32:
		a := value.([]int32)
		cvalue := unsafe.Pointer(&a[0])
		ret = C.tiledb_kv_item_set_value(k.context.tiledbContext, k.tiledbKVItem, cAttribute, cvalue, C.TILEDB_INT32, C.uint64_t(len(a)*int(unsafe.Sizeof(a[0]))))
	case []int64:
		a := value.([]int64)
		cvalue := unsafe.Pointer(&a[0])
		ret = C.tiledb_kv_item_set_value(k.context.tiledbContext, k.tiledbKVItem, cAttribute, cvalue, C.TILEDB_INT64, C.uint64_t(len(a)*int(unsafe.Sizeof(a[0]))))
	case []uint:
		a := value.([]uint)
		cvalue := unsafe.Pointer(&a[0])
		if strconv.IntSize == 32 {
			ret = C.tiledb_kv_item_set_value(k.context.tiledbContext, k.tiledbKVItem, cAttribute, cvalue, C.TILEDB_UINT32, C.uint64_t(len(a)*int(unsafe.Sizeof(a[0]))))
		} else {
			ret = C.tiledb_kv_item_set_value(k.context.tiledbContext, k.tiledbKVItem, cAttribute, cvalue, C.TILEDB_UINT64, C.uint64_t(len(a)*int(unsafe.Sizeof(a[0]))))
		}
	case []uint8:
		a := value.([]uint8)
		cvalue := unsafe.Pointer(&a[0])
		ret = C.tiledb_kv_item_set_value(k.context.tiledbContext, k.tiledbKVItem, cAttribute, cvalue, C.TILEDB_UINT8, C.uint64_t(len(a)*int(unsafe.Sizeof(a[0]))))
	case []uint16:
		a := value.([]uint16)
		cvalue := unsafe.Pointer(&a[0])
		ret = C.tiledb_kv_item_set_value(k.context.tiledbContext, k.tiledbKVItem, cAttribute, cvalue, C.TILEDB_UINT16, C.uint64_t(len(a)*int(unsafe.Sizeof(a[0]))))
	case []uint32:
		a := value.([]uint32)
		cvalue := unsafe.Pointer(&a[0])
		ret = C.tiledb_kv_item_set_value(k.context.tiledbContext, k.tiledbKVItem, cAttribute, cvalue, C.TILEDB_UINT32, C.uint64_t(len(a)*int(unsafe.Sizeof(a[0]))))
	case []uint64:
		a := value.([]uint64)
		cvalue := unsafe.Pointer(&a[0])
		ret = C.tiledb_kv_item_set_value(k.context.tiledbContext, k.tiledbKVItem, cAttribute, cvalue, C.TILEDB_UINT64, C.uint64_t(len(a)*int(unsafe.Sizeof(a[0]))))
	case []float32:
		a := value.([]float32)
		cvalue := unsafe.Pointer(&a[0])
		ret = C.tiledb_kv_item_set_value(k.context.tiledbContext, k.tiledbKVItem, cAttribute, cvalue, C.TILEDB_FLOAT32, C.uint64_t(len(a)*int(unsafe.Sizeof(a[0]))))
	case []float64:
		a := value.([]float64)
		cvalue := unsafe.Pointer(&a[0])
		ret = C.tiledb_kv_item_set_value(k.context.tiledbContext, k.tiledbKVItem, cAttribute, cvalue, C.TILEDB_FLOAT64, C.uint64_t(len(a)*int(unsafe.Sizeof(a[0]))))
	default:
		return fmt.Errorf("Unsupported value type for KVItem: %T", t)
	}

	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error csetting key for KVItem: %s", k.context.LastError())
	}
	return nil
}

// Value Gets the value and value size on a given attribute from a key-value item.
func (k *KVItem) Value(attribute string) (interface{}, error) {
	cAttribute := C.CString(attribute)
	defer C.free(unsafe.Pointer(cAttribute))
	var cValue unsafe.Pointer
	var valueSize C.uint64_t
	var valueType C.tiledb_datatype_t
	ret := C.tiledb_kv_item_get_value(k.context.tiledbContext, k.tiledbKVItem, cAttribute, &cValue, &valueType, &valueSize)

	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error getting value for KVItem: %s", k.context.LastError())
	}

	switch Datatype(valueType) {
	case TILEDB_INT8:
		// If the value size is greater than the size of a single value in bytes it is an array
		elements := int(valueSize) / C.sizeof_int8_t
		if elements > 1 {
			tmpslice := (*[1 << 30]C.int8_t)(unsafe.Pointer(cValue))[:elements:elements]
			retSlice := make([]int8, elements)
			for i, s := range tmpslice {
				retSlice[i] = int8(s)
			}
			return retSlice, nil
		}
		return int8(*(*C.int8_t)(cValue)), nil
	case TILEDB_INT16:
		// If the value size is greater than the size of a single value in bytes it is an array
		elements := int(valueSize) / C.sizeof_int16_t
		if elements > 1 {
			tmpslice := (*[1 << 30]C.int16_t)(unsafe.Pointer(cValue))[:elements:elements]
			retSlice := make([]int16, elements)
			for i, s := range tmpslice {
				retSlice[i] = int16(s)
			}
			return retSlice, nil
		}
		return int16(*(*C.int16_t)(cValue)), nil
	case TILEDB_INT32:
		// If the value size is greater than the size of a single value in bytes it is an array
		elements := int(valueSize) / C.sizeof_int32_t
		if elements > 1 {
			tmpslice := (*[1 << 30]C.int32_t)(unsafe.Pointer(cValue))[:elements:elements]
			retSlice := make([]int32, elements)
			for i, s := range tmpslice {
				retSlice[i] = int32(s)
			}
			return retSlice, nil
		}
		return int32(*(*C.int32_t)(cValue)), nil
	case TILEDB_INT64:
		// If the value size is greater than the size of a single value in bytes it is an array
		elements := int(valueSize) / C.sizeof_int64_t
		if elements > 1 {
			tmpslice := (*[1 << 30]C.int64_t)(unsafe.Pointer(cValue))[:elements:elements]
			retSlice := make([]int64, elements)
			for i, s := range tmpslice {
				retSlice[i] = int64(s)
			}
			return retSlice, nil
		}
		return int64(*(*C.int64_t)(cValue)), nil
	case TILEDB_UINT8:
		// If the value size is greater than the size of a single value in bytes it is an array
		elements := int(valueSize) / C.sizeof_uint8_t
		if elements > 1 {
			tmpslice := (*[1 << 30]C.uint8_t)(unsafe.Pointer(cValue))[:elements:elements]
			retSlice := make([]uint8, elements)
			for i, s := range tmpslice {
				retSlice[i] = uint8(s)
			}
			return retSlice, nil
		}
		return int8(*(*C.uint8_t)(cValue)), nil
	case TILEDB_UINT16:
		// If the value size is greater than the size of a single value in bytes it is an array
		elements := int(valueSize) / C.sizeof_uint16_t
		if elements > 1 {
			tmpslice := (*[1 << 30]C.uint16_t)(unsafe.Pointer(cValue))[:elements:elements]
			retSlice := make([]uint16, elements)
			for i, s := range tmpslice {
				retSlice[i] = uint16(s)
			}
			return retSlice, nil
		}
		return int16(*(*C.uint16_t)(cValue)), nil
	case TILEDB_UINT32:
		// If the value size is greater than the size of a single value in bytes it is an array
		elements := int(valueSize) / C.sizeof_uint32_t
		if elements > 1 {
			tmpslice := (*[1 << 30]C.uint32_t)(unsafe.Pointer(cValue))[:elements:elements]
			retSlice := make([]uint32, elements)
			for i, s := range tmpslice {
				retSlice[i] = uint32(s)
			}
			return retSlice, nil
		}
		return int32(*(*C.uint32_t)(cValue)), nil
	case TILEDB_UINT64:
		// If the value size is greater than the size of a single value in bytes it is an array
		elements := int(valueSize) / C.sizeof_uint64_t
		if elements > 1 {
			tmpslice := (*[1 << 30]C.uint64_t)(unsafe.Pointer(cValue))[:elements:elements]
			retSlice := make([]uint64, elements)
			for i, s := range tmpslice {
				retSlice[i] = uint64(s)
			}
			return retSlice, nil
		}
		return int64(*(*C.uint64_t)(cValue)), nil
	case TILEDB_FLOAT32:
		// If the value size is greater than the size of a single value in bytes it is an array
		elements := int(valueSize) / C.sizeof_float
		if elements > 1 {
			tmpslice := (*[1 << 30]C.float)(unsafe.Pointer(cValue))[:elements:elements]
			retSlice := make([]float32, elements)
			for i, s := range tmpslice {
				retSlice[i] = float32(s)
			}
			return retSlice, nil
		}
		return float32(*(*C.float)(cValue)), nil
	case TILEDB_FLOAT64:
		// If the value size is greater than the size of a single value in bytes it is an array
		elements := int(valueSize) / C.sizeof_double
		if elements > 1 {
			tmpslice := (*[1 << 30]C.double)(unsafe.Pointer(cValue))[:elements:elements]
			retSlice := make([]float64, elements)
			for i, s := range tmpslice {
				retSlice[i] = float64(s)
			}
			return retSlice, nil
		}
		return float64(*(*C.double)(cValue)), nil
	case TILEDB_CHAR:
		elements := int(valueSize) / C.sizeof_char
		return C.GoStringN((*C.char)(cValue), C.int32_t(elements)), nil
	default:
		return nil, fmt.Errorf("Unsupported tiledb value type: %v", valueType)
	}

	return nil, fmt.Errorf("Error getting value for KVItem")
}
