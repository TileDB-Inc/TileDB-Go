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
	"os"
	"reflect"
	"runtime"
	"strconv"
	"unsafe"
)

// Dimension Describes one dimension of an Array.
// The dimension consists of a type, lower and upper bound, and tile-extent
// describing the memory ordering. Dimensions are added to a Domain.
type Dimension struct {
	tiledbDimension *C.tiledb_dimension_t
	context         *Context
}

// NewDimension alloc a new dimension
func NewDimension(context *Context, name string, datatype Datatype, domain interface{}, extent interface{}) (*Dimension, error) {
	dimension := Dimension{context: context}
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))

	if reflect.TypeOf(domain).Kind() != reflect.Slice {
		return nil, fmt.Errorf("Domain passed must be a slice of two integers or two floats, type passed was: %s", reflect.TypeOf(domain).Kind().String())
	}
	domainInterfaceVal := reflect.ValueOf(domain)

	if domainInterfaceVal.Len() != 2 {
		return nil, fmt.Errorf("Domain passed must be a slice of two integers or two floats, size of slice is: %d", domainInterfaceVal.Len())
	}

	domainType := reflect.TypeOf(domain).Elem().Kind()
	extentType := reflect.TypeOf(extent).Kind()
	if extentType != domainType {
		return nil, fmt.Errorf("Domaing and extent do not have the same data types. Domain: %s, Extent: %s", domainType.String(), extentType.String())
	}

	// Domain data type need to match datatype passed
	domainTypeMatchDatatype := true

	var ret C.int32_t
	// Convert domain to type then to void*
	var cdomain unsafe.Pointer
	// Convert extent to type then to void*
	var cextent unsafe.Pointer
	// Switch on datatype type to create void* for domain and extent.
	// Extent has already checked to be same type as domain so this is safe
	switch datatype {
	case TILEDB_INT8:
		if domainType != reflect.Int8 {
			domainTypeMatchDatatype = false
			break
		}
		// Create domain void*
		tmpDomain := domain.([]int8)
		cdomain = unsafe.Pointer(&tmpDomain[0])
		// Create extent void*
		tmpExtent := (extent.(int8))
		cextent = unsafe.Pointer(&tmpExtent)
	case TILEDB_INT16:
		if domainType != reflect.Int16 {
			domainTypeMatchDatatype = false
			break
		}
		// Create domain void*
		tmpDomain := domain.([]int16)
		cdomain = unsafe.Pointer(&tmpDomain[0])
		// Create extent void*
		tmpExtent := (extent.(int16))
		cextent = unsafe.Pointer(&tmpExtent)
	case TILEDB_INT32:
		if domainType != reflect.Int32 && domainType != reflect.Int {
			domainTypeMatchDatatype = false
			break
		}
		if domainType == reflect.Int && strconv.IntSize == 64 {
			// User asked for Int64 if size of int on platform is 64 bit
			domainTypeMatchDatatype = false
			break
		}
		// Create domain void*
		tmpDomain := domain.([]int32)
		cdomain = unsafe.Pointer(&tmpDomain[0])
		// Create extent void*
		tmpExtent := (extent.(int32))
		cextent = unsafe.Pointer(&tmpExtent)
	case TILEDB_INT64, TILEDB_DATETIME_YEAR, TILEDB_DATETIME_MONTH, TILEDB_DATETIME_WEEK, TILEDB_DATETIME_DAY, TILEDB_DATETIME_HR, TILEDB_DATETIME_MIN, TILEDB_DATETIME_SEC, TILEDB_DATETIME_MS, TILEDB_DATETIME_US, TILEDB_DATETIME_NS, TILEDB_DATETIME_PS, TILEDB_DATETIME_FS, TILEDB_DATETIME_AS, TILEDB_TIME_HR, TILEDB_TIME_MIN, TILEDB_TIME_SEC, TILEDB_TIME_MS, TILEDB_TIME_US, TILEDB_TIME_NS, TILEDB_TIME_PS, TILEDB_TIME_FS, TILEDB_TIME_AS:
		if domainType != reflect.Int64 && domainType != reflect.Int {
			domainTypeMatchDatatype = false
			break
		}
		if domainType == reflect.Int && strconv.IntSize == 32 {
			// User asked for Int32 if size of int on platform is 32 bit
			domainTypeMatchDatatype = false
			break
		}
		// Create domain void*
		tmpDomain := domain.([]int64)
		cdomain = unsafe.Pointer(&tmpDomain[0])
		// Create extent void*
		tmpExtent := (extent.(int64))
		cextent = unsafe.Pointer(&tmpExtent)
	case TILEDB_UINT8:
		if domainType != reflect.Uint8 {
			domainTypeMatchDatatype = false
			break
		}
		// Create domain void*
		tmpDomain := domain.([]uint8)
		cdomain = unsafe.Pointer(&tmpDomain[0])
		// Create extent void*
		tmpExtent := (extent.(uint8))
		cextent = unsafe.Pointer(&tmpExtent)
	case TILEDB_UINT16:
		if domainType != reflect.Uint16 {
			domainTypeMatchDatatype = false
			break
		}
		// Create domain void*
		tmpDomain := domain.([]uint16)
		cdomain = unsafe.Pointer(&tmpDomain[0])
		// Create extent void*
		tmpExtent := (extent.(uint16))
		cextent = unsafe.Pointer(&tmpExtent)
	case TILEDB_UINT32:
		if domainType != reflect.Uint32 && domainType != reflect.Uint {
			domainTypeMatchDatatype = false
			break
		}
		if domainType == reflect.Uint && strconv.IntSize == 64 {
			// User asked for Uint64 if size of int on platform is 64 bit
			domainTypeMatchDatatype = false
			break
		}
		// Create domain void*
		tmpDomain := domain.([]uint32)
		cdomain = unsafe.Pointer(&tmpDomain[0])
		// Create extent void*
		tmpExtent := (extent.(uint32))
		cextent = unsafe.Pointer(&tmpExtent)
	case TILEDB_UINT64:
		if domainType != reflect.Uint64 && domainType != reflect.Uint {
			domainTypeMatchDatatype = false
			break
		}
		if domainType == reflect.Uint && strconv.IntSize == 32 {
			// User asked for Uint32 if size of int on platform is 32 bit
			domainTypeMatchDatatype = false
			break
		}
		// Create domain void*
		tmpDomain := domain.([]uint64)
		cdomain = unsafe.Pointer(&tmpDomain[0])
		// Create extent void*
		tmpExtent := (extent.(uint64))
		cextent = unsafe.Pointer(&tmpExtent)
	case TILEDB_FLOAT32:
		if domainType != reflect.Float32 {
			domainTypeMatchDatatype = false
			break
		}
		// Create domain void*
		tmpDomain := domain.([]float32)
		cdomain = unsafe.Pointer(&tmpDomain[0])
		// Create extent void*
		tmpExtent := (extent.(float32))
		cextent = unsafe.Pointer(&tmpExtent)
	case TILEDB_FLOAT64:
		if domainType != reflect.Float64 {
			domainTypeMatchDatatype = false
			break
		}
		// Create domain void*
		tmpDomain := domain.([]float64)
		cdomain = unsafe.Pointer(&tmpDomain[0])
		// Create extent void*
		tmpExtent := (extent.(float64))
		cextent = unsafe.Pointer(&tmpExtent)
	default:
		return nil, fmt.Errorf("Unrecognized datatype passed: %s", datatype.String())
	}

	if !domainTypeMatchDatatype {
		return nil, fmt.Errorf("domain and datatype do not have the same data types. Domain: %s, Datatype: %s", domainType.String(), datatype.String())
	}

	ret = C.tiledb_dimension_alloc(context.tiledbContext, cname, C.tiledb_datatype_t(datatype), cdomain, cextent, &dimension.tiledbDimension)

	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("error creating tiledb dimension: %s", context.LastError())
	}

	// Set finalizer for free C pointer on gc
	runtime.SetFinalizer(&dimension, func(dimension *Dimension) {
		dimension.Free()
	})

	return &dimension, nil
}

// NewStringDimension alloc a new string dimension
func NewStringDimension(context *Context, name string) (*Dimension, error) {
	dimension := Dimension{context: context}
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))

	var datatype Datatype
	var ret C.int32_t

	datatype = TILEDB_STRING_ASCII
	ret = C.tiledb_dimension_alloc(context.tiledbContext, cname, C.tiledb_datatype_t(datatype), nil, nil, &dimension.tiledbDimension)

	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error creating tiledb dimension: %s", context.LastError())
	}

	// Set finalizer for free C pointer on gc
	runtime.SetFinalizer(&dimension, func(dimension *Dimension) {
		dimension.Free()
	})

	return &dimension, nil
}

// Free releases the internal TileDB core data that was allocated on the C heap.
// It is automatically called when this object is garbage collected, but can be
// called earlier to manually release memory if needed. Free is idempotent and
// can safely be called many times on the same object; if it has already
// been freed, it will not be freed again.
func (d *Dimension) Free() {
	if d.tiledbDimension != nil {
		C.tiledb_dimension_free(&d.tiledbDimension)
	}
}

// SetFilterList sets the dimension filterList
func (d *Dimension) SetFilterList(filterlist *FilterList) error {
	ret := C.tiledb_dimension_set_filter_list(d.context.tiledbContext, d.tiledbDimension, filterlist.tiledbFilterList)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error setting tiledb dimension filter list: %s", d.context.LastError())
	}
	return nil
}

// FilterList returns a copy of the filter list for attribute
func (d *Dimension) FilterList() (*FilterList, error) {
	filterList := FilterList{context: d.context}
	ret := C.tiledb_dimension_get_filter_list(d.context.tiledbContext, d.tiledbDimension, &filterList.tiledbFilterList)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error getting tiledb dimension filter list: %s", d.context.LastError())
	}

	// Set finalizer for free C pointer on gc
	runtime.SetFinalizer(&filterList, func(filterList *FilterList) {
		filterList.Free()
	})

	return &filterList, nil
}

// SetCellValNum Sets the number of values per cell for a dimension.
// If this is not used, the default is `1`.
// This is inferred from the type parameter of the NewDimension
// function, but can also be set manually.
func (d *Dimension) SetCellValNum(val uint32) error {
	ret := C.tiledb_dimension_set_cell_val_num(d.context.tiledbContext,
		d.tiledbDimension, C.uint32_t(val))
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error setting tiledb dimension cell val num: %s", d.context.LastError())
	}
	return nil
}

// CellValNum returns number of values of one cell on this attribute.
// For variable-sized attributes returns TILEDB_VAR_NUM.
func (d *Dimension) CellValNum() (uint32, error) {
	var cellValNum C.uint32_t
	ret := C.tiledb_dimension_get_cell_val_num(d.context.tiledbContext, d.tiledbDimension, &cellValNum)
	if ret != C.TILEDB_OK {
		return 0, fmt.Errorf("Error getting tiledb dimension cell val num: %s", d.context.LastError())
	}

	return uint32(cellValNum), nil
}

// Name returns the name of the dimension
func (d *Dimension) Name() (string, error) {
	var cName *C.char
	ret := C.tiledb_dimension_get_name(d.context.tiledbContext, d.tiledbDimension, &cName)
	if ret != C.TILEDB_OK {
		return "", fmt.Errorf("Error getting tiledb dimension name: %s", d.context.LastError())
	}

	return C.GoString(cName), nil
}

// Type returns the type of the dimension
func (d *Dimension) Type() (Datatype, error) {
	var cType C.tiledb_datatype_t
	ret := C.tiledb_dimension_get_type(d.context.tiledbContext, d.tiledbDimension, &cType)
	if ret != C.TILEDB_OK {
		return 0, fmt.Errorf("Error getting tiledb dimension type: %s", d.context.LastError())
	}

	return Datatype(cType), nil
}

// Domain returns the dimension's domain
func (d *Dimension) Domain() (interface{}, error) {
	datatype, err := d.Type()
	if err != nil {
		return nil, err
	}

	var ret C.int32_t
	var domain interface{}
	switch datatype {
	case TILEDB_INT8:
		cdomain := C.malloc(2 * C.sizeof_int8_t)
		defer C.free(cdomain)
		tmpDomain := make([]int8, 2)
		ret = C.tiledb_dimension_get_domain(d.context.tiledbContext, d.tiledbDimension, &cdomain)
		tmpslice := (*[1 << 46]C.int8_t)(unsafe.Pointer(cdomain))[:2:2]
		for i, s := range tmpslice {
			tmpDomain[i] = int8(s)
		}
		domain = tmpDomain
	case TILEDB_INT16:
		cdomain := C.malloc(2 * C.sizeof_int16_t)
		defer C.free(cdomain)
		tmpDomain := make([]int16, 2)
		ret = C.tiledb_dimension_get_domain(d.context.tiledbContext, d.tiledbDimension, &cdomain)
		tmpslice := (*[1 << 46]C.int16_t)(unsafe.Pointer(cdomain))[:2:2]
		for i, s := range tmpslice {
			tmpDomain[i] = int16(s)
		}
		domain = tmpDomain
	case TILEDB_INT32:
		cdomain := C.malloc(2 * C.sizeof_int32_t)
		defer C.free(cdomain)
		tmpDomain := make([]int32, 2)
		ret = C.tiledb_dimension_get_domain(d.context.tiledbContext, d.tiledbDimension, &cdomain)
		tmpslice := (*[1 << 46]C.int32_t)(unsafe.Pointer(cdomain))[:2:2]
		for i, s := range tmpslice {
			tmpDomain[i] = int32(s)
		}
		domain = tmpDomain
	case TILEDB_INT64:
		cdomain := C.malloc(2 * C.sizeof_int64_t)
		defer C.free(cdomain)
		tmpDomain := make([]int64, 2)
		ret = C.tiledb_dimension_get_domain(d.context.tiledbContext, d.tiledbDimension, &cdomain)
		tmpslice := (*[1 << 46]C.int64_t)(unsafe.Pointer(cdomain))[:2:2]
		for i, s := range tmpslice {
			tmpDomain[i] = int64(s)
		}
		domain = tmpDomain
	case TILEDB_UINT8:
		cdomain := C.malloc(2 * C.sizeof_uint8_t)
		defer C.free(cdomain)
		tmpDomain := make([]uint8, 2)
		ret = C.tiledb_dimension_get_domain(d.context.tiledbContext, d.tiledbDimension, &cdomain)
		tmpslice := (*[1 << 46]C.uint8_t)(unsafe.Pointer(cdomain))[:2:2]
		for i, s := range tmpslice {
			tmpDomain[i] = uint8(s)
		}
		domain = tmpDomain
	case TILEDB_UINT16:
		cdomain := C.malloc(2 * C.sizeof_uint16_t)
		defer C.free(cdomain)
		tmpDomain := make([]uint16, 2)
		ret = C.tiledb_dimension_get_domain(d.context.tiledbContext, d.tiledbDimension, &cdomain)
		tmpslice := (*[1 << 46]C.uint16_t)(unsafe.Pointer(cdomain))[:2:2]
		for i, s := range tmpslice {
			tmpDomain[i] = uint16(s)
		}
		domain = tmpDomain
	case TILEDB_UINT32:
		cdomain := C.malloc(2 * C.sizeof_uint32_t)
		defer C.free(cdomain)
		tmpDomain := make([]uint32, 2)
		ret = C.tiledb_dimension_get_domain(d.context.tiledbContext, d.tiledbDimension, &cdomain)
		tmpslice := (*[1 << 46]C.uint32_t)(unsafe.Pointer(cdomain))[:2:2]
		for i, s := range tmpslice {
			tmpDomain[i] = uint32(s)
		}
		domain = tmpDomain
	case TILEDB_UINT64:
		cdomain := C.malloc(2 * C.sizeof_uint64_t)
		defer C.free(cdomain)
		tmpDomain := make([]uint64, 2)
		ret = C.tiledb_dimension_get_domain(d.context.tiledbContext, d.tiledbDimension, &cdomain)
		tmpslice := (*[1 << 46]C.uint64_t)(unsafe.Pointer(cdomain))[:2:2]
		for i, s := range tmpslice {
			tmpDomain[i] = uint64(s)
		}
		domain = tmpDomain
	case TILEDB_FLOAT32:
		cdomain := C.malloc(2 * C.sizeof_float)
		defer C.free(cdomain)
		tmpDomain := make([]float32, 2)
		ret = C.tiledb_dimension_get_domain(d.context.tiledbContext, d.tiledbDimension, &cdomain)
		tmpslice := (*[1 << 46]C.float)(unsafe.Pointer(cdomain))[:2:2]
		for i, s := range tmpslice {
			tmpDomain[i] = float32(s)
		}
		domain = tmpDomain
	case TILEDB_FLOAT64:
		cdomain := C.malloc(2 * C.sizeof_double)
		defer C.free(cdomain)
		tmpDomain := make([]float64, 2)
		ret = C.tiledb_dimension_get_domain(d.context.tiledbContext, d.tiledbDimension, &cdomain)
		tmpslice := (*[1 << 46]C.double)(unsafe.Pointer(cdomain))[:2:2]
		for i, s := range tmpslice {
			tmpDomain[i] = float64(s)
		}
		domain = tmpDomain
	case TILEDB_STRING_ASCII:
		domain = nil
	default:
		return nil, fmt.Errorf("Unrecognized domain type: %d", datatype)
	}
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error getting tiledb dimension's domain: %s", d.context.LastError())
	}

	return domain, nil
}

// Extent returns the dimension's extent
func (d *Dimension) Extent() (interface{}, error) {
	datatype, err := d.Type()
	if err != nil {
		return nil, err
	}

	var ret C.int32_t
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
	case TILEDB_STRING_ASCII:
		extent = nil
	default:
		return nil, fmt.Errorf("Unrecognized extent type: %d", datatype)
	}
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error getting tiledb dimension's extent: %s", d.context.LastError())
	}

	return extent, nil
}

// DumpSTDOUT Dumps the dimension in ASCII format to stdout
func (d *Dimension) DumpSTDOUT() error {
	ret := C.tiledb_dimension_dump(d.context.tiledbContext, d.tiledbDimension, C.stdout)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error dumping dimension to stdout: %s", d.context.LastError())
	}
	return nil
}

// Dump Dumps the dimension in ASCII format in the selected output.
func (d *Dimension) Dump(path string) error {

	if _, err := os.Stat(path); err == nil {
		return fmt.Errorf("Error path already %s exists", path)
	}

	// Convert to char *
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))

	// Set mode as char*
	cMode := C.CString("w")
	defer C.free(unsafe.Pointer(cMode))

	// Open file to get FILE*
	cFile := C.fopen(cPath, cMode)
	defer C.fclose(cFile)

	// Dump dimension to file
	ret := C.tiledb_dimension_dump(d.context.tiledbContext, d.tiledbDimension, cFile)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error dumping dimension to file %s: %s", path, d.context.LastError())
	}
	return nil
}
