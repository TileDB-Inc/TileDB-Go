package tiledb

/*
#cgo LDFLAGS: -ltiledb
#include <tiledb/tiledb.h>
#include <stdlib.h>
*/
import "C"

import (
	"fmt"
	"reflect"
	"runtime"
	"strconv"
	"unsafe"
)

// Dimension is tiledb dimension
type Dimension struct {
	tiledbDimension *C.tiledb_dimension_t
	context         *Context
}

// NewDimension alloc a new dimension
func NewDimension(context *Context, name string, domain interface{}, extent interface{}) (*Dimension, error) {
	dimension := Dimension{context: context}
	var cname *C.char = C.CString(name)
	defer C.free(unsafe.Pointer(cname))

	if reflect.TypeOf(domain).Kind() != reflect.Slice {
		return nil, fmt.Errorf("Domain passed must be a slice of two integers or two floats, type passed was: %s", reflect.TypeOf(domain).Kind().String())
	}
	domainInterfaceVal := reflect.ValueOf(domain)

	if domainInterfaceVal.Len() != 2 {
		return nil, fmt.Errorf("Domain passed must be a slice of two integers or two floats, size of slice is: %d", domainInterfaceVal.Len())
	}

	var datatype Datatype
	var ret C.int
	var cdomain unsafe.Pointer
	// Convert domain to type then to void*
	switch domainInterfaceVal.Index(0).Kind() {
	case reflect.Int:
		// Check size of uint on platform
		if strconv.IntSize == 32 {
			datatype = TILEDB_INT32
		} else {
			datatype = TILEDB_INT64
		}
		tmpDomain := domain.([]int)
		cdomain = unsafe.Pointer(&tmpDomain[0])
	case reflect.Int8:
		datatype = TILEDB_INT8
		tmpDomain := domain.([]int8)
		cdomain = unsafe.Pointer(&tmpDomain[0])
	case reflect.Int16:
		datatype = TILEDB_INT16
		tmpDomain := domain.([]int16)
		cdomain = unsafe.Pointer(&tmpDomain[0])
	case reflect.Int32:
		datatype = TILEDB_INT32
		tmpDomain := domain.([]int32)
		cdomain = unsafe.Pointer(&tmpDomain[0])
	case reflect.Int64:
		datatype = TILEDB_INT64
		tmpDomain := domain.([]int64)
		cdomain = unsafe.Pointer(&tmpDomain[0])
	case reflect.Uint:
		// Check size of uint on platform
		if strconv.IntSize == 32 {
			datatype = TILEDB_UINT32
		} else {
			datatype = TILEDB_UINT64
		}
		tmpDomain := domain.([]uint)
		cdomain = unsafe.Pointer(&tmpDomain[0])
	case reflect.Uint8:
		datatype = TILEDB_UINT8
		tmpDomain := domain.([]uint8)
		cdomain = unsafe.Pointer(&tmpDomain[0])
	case reflect.Uint16:
		datatype = TILEDB_UINT16
		tmpDomain := domain.([]uint16)
		cdomain = unsafe.Pointer(&tmpDomain[0])
	case reflect.Uint32:
		datatype = TILEDB_UINT32
		tmpDomain := domain.([]uint32)
		cdomain = unsafe.Pointer(&tmpDomain[0])
	case reflect.Uint64:
		datatype = TILEDB_UINT64
		tmpDomain := domain.([]uint64)
		cdomain = unsafe.Pointer(&tmpDomain[0])
	case reflect.Float32:
		datatype = TILEDB_FLOAT32
		tmpDomain := domain.([]float32)
		cdomain = unsafe.Pointer(&tmpDomain[0])
	case reflect.Float64:
		datatype = TILEDB_FLOAT64
		tmpDomain := domain.([]float64)
		cdomain = unsafe.Pointer(&tmpDomain[0])
	default:
		return nil, fmt.Errorf("Unrecognized domain type passed: %s", domainInterfaceVal.Index(0).Kind().String())
	}

	// Convert extent to type then to void*
	var cextent unsafe.Pointer
	switch reflect.ValueOf(extent).Kind() {
	case reflect.Int:
		tmpExtent := (extent.(int))
		cextent = unsafe.Pointer(&tmpExtent)
	case reflect.Int8:
		tmpExtent := (extent.(int8))
		cextent = unsafe.Pointer(&tmpExtent)
	case reflect.Int16:
		tmpExtent := (extent.(int16))
		cextent = unsafe.Pointer(&tmpExtent)
	case reflect.Int32:
		tmpExtent := (extent.(int32))
		cextent = unsafe.Pointer(&tmpExtent)
	case reflect.Int64:
		tmpExtent := (extent.(int64))
		cextent = unsafe.Pointer(&tmpExtent)
	case reflect.Uint:
		tmpExtent := (extent.(uint))
		cextent = unsafe.Pointer(&tmpExtent)
	case reflect.Uint8:
		tmpExtent := (extent.(uint8))
		cextent = unsafe.Pointer(&tmpExtent)
	case reflect.Uint16:
		tmpExtent := (extent.(uint16))
		cextent = unsafe.Pointer(&tmpExtent)
	case reflect.Uint32:
		tmpExtent := (extent.(uint32))
		cextent = unsafe.Pointer(&tmpExtent)
	case reflect.Uint64:
		tmpExtent := (extent.(uint64))
		cextent = unsafe.Pointer(&tmpExtent)
	case reflect.Float32:
		tmpExtent := (extent.(float32))
		cextent = unsafe.Pointer(&tmpExtent)
	case reflect.Float64:
		tmpExtent := (extent.(float64))
		cextent = unsafe.Pointer(&tmpExtent)
	default:
		return nil, fmt.Errorf("Unrecognized extent type passed: %s", reflect.ValueOf(extent).Kind().String())
	}

	ret = C.tiledb_dimension_alloc(context.tiledbContext, cname, C.tiledb_datatype_t(datatype), cdomain, cextent, &dimension.tiledbDimension)

	if ret == C.TILEDB_ERR {
		return nil, fmt.Errorf("Error creating tiledb dimension: %s", context.GetLastError())
	}

	// Set finalizer for free C pointer on gc
	runtime.SetFinalizer(&dimension, func(dimension *Dimension) {
		dimension.Free()
	})

	return &dimension, nil
}

// Free tiledb_dimension_t that was allocated on heap in c
func (d *Dimension) Free() {
	if d.tiledbDimension != nil {
		C.tiledb_dimension_free(&d.tiledbDimension)
	}
}

// Name returns the name of the dimension
func (d *Dimension) Name() (string, error) {
	var cName *C.char
	defer C.free(unsafe.Pointer(cName))
	ret := C.tiledb_dimension_get_name(d.context.tiledbContext, d.tiledbDimension, &cName)
	if ret == C.TILEDB_ERR {
		return "", fmt.Errorf("Error getting tiledb dimension name: %s", d.context.GetLastError())
	}

	return C.GoString(cName), nil
}

// Type returns the type of the dimension
func (d *Dimension) Type() (Datatype, error) {
	var cType C.tiledb_datatype_t
	ret := C.tiledb_dimension_get_type(d.context.tiledbContext, d.tiledbDimension, &cType)
	if ret == C.TILEDB_ERR {
		return 0, fmt.Errorf("Error getting tiledb dimension type: %s", d.context.GetLastError())
	}

	return Datatype(cType), nil
}

// Domain returns the dimension's domain
func (d *Dimension) Domain() (interface{}, error) {
	datatype, err := d.Type()
	if err != nil {
		return nil, err
	}

	var ret C.int
	var domain interface{}
	switch datatype {
	case TILEDB_INT8:
		cdomain := C.malloc(2 * C.sizeof_int8_t)
		defer C.free(cdomain)
		tmpDomain := make([]int8, 2)
		ret = C.tiledb_dimension_get_domain(d.context.tiledbContext, d.tiledbDimension, &cdomain)
		tmpslice := (*[1 << 30]C.int8_t)(unsafe.Pointer(cdomain))[:2:2]
		for i, s := range tmpslice {
			tmpDomain[i] = int8(s)
		}
		domain = tmpDomain
	case TILEDB_INT16:
		cdomain := C.malloc(2 * C.sizeof_int16_t)
		defer C.free(cdomain)
		tmpDomain := make([]int16, 2)
		ret = C.tiledb_dimension_get_domain(d.context.tiledbContext, d.tiledbDimension, &cdomain)
		tmpslice := (*[1 << 30]C.int16_t)(unsafe.Pointer(cdomain))[:2:2]
		for i, s := range tmpslice {
			tmpDomain[i] = int16(s)
		}
		domain = tmpDomain
	case TILEDB_INT32:
		cdomain := C.malloc(2 * C.sizeof_int32_t)
		defer C.free(cdomain)
		tmpDomain := make([]int32, 2)
		ret = C.tiledb_dimension_get_domain(d.context.tiledbContext, d.tiledbDimension, &cdomain)
		tmpslice := (*[1 << 30]C.int32_t)(unsafe.Pointer(cdomain))[:2:2]
		for i, s := range tmpslice {
			tmpDomain[i] = int32(s)
		}
		domain = tmpDomain
	case TILEDB_INT64:
		cdomain := C.malloc(2 * C.sizeof_int64_t)
		defer C.free(cdomain)
		tmpDomain := make([]int64, 2)
		ret = C.tiledb_dimension_get_domain(d.context.tiledbContext, d.tiledbDimension, &cdomain)
		tmpslice := (*[1 << 30]C.int64_t)(unsafe.Pointer(cdomain))[:2:2]
		for i, s := range tmpslice {
			tmpDomain[i] = int64(s)
		}
		domain = tmpDomain
	case TILEDB_UINT8:
		cdomain := C.malloc(2 * C.sizeof_uint8_t)
		defer C.free(cdomain)
		tmpDomain := make([]uint8, 2)
		ret = C.tiledb_dimension_get_domain(d.context.tiledbContext, d.tiledbDimension, &cdomain)
		tmpslice := (*[1 << 30]C.uint8_t)(unsafe.Pointer(cdomain))[:2:2]
		for i, s := range tmpslice {
			tmpDomain[i] = uint8(s)
		}
		domain = tmpDomain
	case TILEDB_UINT16:
		cdomain := C.malloc(2 * C.sizeof_uint16_t)
		defer C.free(cdomain)
		tmpDomain := make([]uint16, 2)
		ret = C.tiledb_dimension_get_domain(d.context.tiledbContext, d.tiledbDimension, &cdomain)
		tmpslice := (*[1 << 30]C.uint16_t)(unsafe.Pointer(cdomain))[:2:2]
		for i, s := range tmpslice {
			tmpDomain[i] = uint16(s)
		}
		domain = tmpDomain
	case TILEDB_UINT32:
		cdomain := C.malloc(2 * C.sizeof_uint32_t)
		defer C.free(cdomain)
		tmpDomain := make([]uint32, 2)
		ret = C.tiledb_dimension_get_domain(d.context.tiledbContext, d.tiledbDimension, &cdomain)
		tmpslice := (*[1 << 30]C.uint32_t)(unsafe.Pointer(cdomain))[:2:2]
		for i, s := range tmpslice {
			tmpDomain[i] = uint32(s)
		}
		domain = tmpDomain
	case TILEDB_UINT64:
		cdomain := C.malloc(2 * C.sizeof_uint64_t)
		defer C.free(cdomain)
		tmpDomain := make([]uint64, 2)
		ret = C.tiledb_dimension_get_domain(d.context.tiledbContext, d.tiledbDimension, &cdomain)
		tmpslice := (*[1 << 30]C.uint64_t)(unsafe.Pointer(cdomain))[:2:2]
		for i, s := range tmpslice {
			tmpDomain[i] = uint64(s)
		}
		domain = tmpDomain
	case TILEDB_FLOAT32:
		cdomain := C.malloc(2 * C.sizeof_float)
		defer C.free(cdomain)
		tmpDomain := make([]float32, 2)
		ret = C.tiledb_dimension_get_domain(d.context.tiledbContext, d.tiledbDimension, &cdomain)
		tmpslice := (*[1 << 30]C.float)(unsafe.Pointer(cdomain))[:2:2]
		for i, s := range tmpslice {
			tmpDomain[i] = float32(s)
		}
		domain = tmpDomain
	case TILEDB_FLOAT64:
		cdomain := C.malloc(2 * C.sizeof_double)
		defer C.free(cdomain)
		tmpDomain := make([]float64, 2)
		ret = C.tiledb_dimension_get_domain(d.context.tiledbContext, d.tiledbDimension, &cdomain)
		tmpslice := (*[1 << 30]C.double)(unsafe.Pointer(cdomain))[:2:2]
		for i, s := range tmpslice {
			tmpDomain[i] = float64(s)
		}
		domain = tmpDomain
	default:
		return nil, fmt.Errorf("Unrecognized domain type: %d", datatype)
	}
	if ret == C.TILEDB_ERR {
		return nil, fmt.Errorf("Error getting tiledb dimension's domain: %s", d.context.GetLastError())
	}

	return domain, nil
}

// Extent returns the dimension's extent
func (d *Dimension) Extent() (interface{}, error) {
	datatype, err := d.Type()
	if err != nil {
		return nil, err
	}

	var ret C.int
	var extent interface{}
	switch datatype {
	case TILEDB_INT8:
		cextent := C.malloc(C.sizeof_int8_t)
		defer C.free(cextent)
		ret = C.tiledb_dimension_get_tile_extent(d.context.tiledbContext, d.tiledbDimension, &cextent)
		extent = *(*int8)(unsafe.Pointer(cextent))
	case TILEDB_INT16:
		cextent := C.malloc(C.sizeof_int16_t)
		defer C.free(cextent)
		ret = C.tiledb_dimension_get_tile_extent(d.context.tiledbContext, d.tiledbDimension, &cextent)
		extent = *(*int16)(unsafe.Pointer(cextent))
	case TILEDB_INT32:
		cextent := C.malloc(C.sizeof_int32_t)
		defer C.free(cextent)
		ret = C.tiledb_dimension_get_tile_extent(d.context.tiledbContext, d.tiledbDimension, &cextent)
		extent = *(*int32)(unsafe.Pointer(cextent))
	case TILEDB_INT64:
		cextent := C.malloc(C.sizeof_int64_t)
		defer C.free(cextent)
		ret = C.tiledb_dimension_get_tile_extent(d.context.tiledbContext, d.tiledbDimension, &cextent)
		extent = *(*int64)(unsafe.Pointer(cextent))
	case TILEDB_UINT8:
		cextent := C.malloc(C.sizeof_uint8_t)
		defer C.free(cextent)
		ret = C.tiledb_dimension_get_tile_extent(d.context.tiledbContext, d.tiledbDimension, &cextent)
		extent = *(*uint8)(unsafe.Pointer(cextent))
	case TILEDB_UINT16:
		cextent := C.malloc(C.sizeof_uint16_t)
		defer C.free(cextent)
		ret = C.tiledb_dimension_get_tile_extent(d.context.tiledbContext, d.tiledbDimension, &cextent)
		extent = *(*uint16)(unsafe.Pointer(cextent))
	case TILEDB_UINT32:
		cextent := C.malloc(C.sizeof_uint32_t)
		defer C.free(cextent)
		ret = C.tiledb_dimension_get_tile_extent(d.context.tiledbContext, d.tiledbDimension, &cextent)
		extent = *(*uint32)(unsafe.Pointer(cextent))
	case TILEDB_UINT64:
		cextent := C.malloc(C.sizeof_uint64_t)
		defer C.free(cextent)
		ret = C.tiledb_dimension_get_tile_extent(d.context.tiledbContext, d.tiledbDimension, &cextent)
		extent = *(*uint64)(unsafe.Pointer(cextent))
	case TILEDB_FLOAT32:
		cextent := C.malloc(C.sizeof_float)
		defer C.free(cextent)
		ret = C.tiledb_dimension_get_tile_extent(d.context.tiledbContext, d.tiledbDimension, &cextent)
		extent = *(*float32)(unsafe.Pointer(cextent))
	case TILEDB_FLOAT64:
		cextent := C.malloc(C.sizeof_double)
		defer C.free(cextent)
		ret = C.tiledb_dimension_get_tile_extent(d.context.tiledbContext, d.tiledbDimension, &cextent)
		extent = *(*float64)(unsafe.Pointer(cextent))
	default:
		return nil, fmt.Errorf("Unrecognized extent type: %d", datatype)
	}
	if ret == C.TILEDB_ERR {
		return nil, fmt.Errorf("Error getting tiledb dimension's extent: %s", d.context.GetLastError())
	}

	return extent, nil
}
