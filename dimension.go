package tiledb

/*
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

// NewDimension allocates a new dimension.
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
		return nil, fmt.Errorf("domain and extent do not have the same data types. Domain: %s, Extent: %s", domainType, extentType)
	}

	// Domain data type need to match datatype passed
	domainTypeMatchDatatype := true

	var ret C.int32_t
	// Convert domain to type then to void*
	// Use domainPtr to ensure cdomain is not collected before it is passed to tiledb.
	var domainPtr any
	defer runtime.KeepAlive(domainPtr)
	var cdomain unsafe.Pointer
	// Convert extent to type then to void*
	// Use extentPtr to ensure cdomain is not collected before it is passed to tiledb.
	var extentPtr any
	defer runtime.KeepAlive(extentPtr)
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
		domainPtr = &tmpDomain
		cdomain = unsafe.Pointer(&tmpDomain[0])
		// Create extent void*
		tmpExtent := extent.(int8)
		extentPtr = &tmpExtent
		cextent = unsafe.Pointer(&tmpExtent)
	case TILEDB_INT16:
		if domainType != reflect.Int16 {
			domainTypeMatchDatatype = false
			break
		}
		// Create domain void*
		tmpDomain := domain.([]int16)
		domainPtr = &tmpDomain
		cdomain = unsafe.Pointer(&tmpDomain[0])
		// Create extent void*
		tmpExtent := extent.(int16)
		extentPtr = &tmpExtent
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
		domainPtr = &tmpDomain
		cdomain = unsafe.Pointer(&tmpDomain[0])
		// Create extent void*
		tmpExtent := extent.(int32)
		extentPtr = &tmpExtent
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
		domainPtr = &tmpDomain
		cdomain = unsafe.Pointer(&tmpDomain[0])
		// Create extent void*
		tmpExtent := extent.(int64)
		extentPtr = &tmpExtent
		cextent = unsafe.Pointer(&tmpExtent)
	case TILEDB_UINT8:
		if domainType != reflect.Uint8 {
			domainTypeMatchDatatype = false
			break
		}
		// Create domain void*
		tmpDomain := domain.([]uint8)
		domainPtr = &tmpDomain
		cdomain = unsafe.Pointer(&tmpDomain[0])
		// Create extent void*
		tmpExtent := extent.(uint8)
		extentPtr = &tmpExtent
		cextent = unsafe.Pointer(&tmpExtent)
	case TILEDB_UINT16:
		if domainType != reflect.Uint16 {
			domainTypeMatchDatatype = false
			break
		}
		// Create domain void*
		tmpDomain := domain.([]uint16)
		domainPtr = &tmpDomain
		cdomain = unsafe.Pointer(&tmpDomain[0])
		// Create extent void*
		tmpExtent := extent.(uint16)
		extentPtr = &tmpExtent
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
		domainPtr = &tmpDomain
		cdomain = unsafe.Pointer(&tmpDomain[0])
		// Create extent void*
		tmpExtent := extent.(uint32)
		extentPtr = &tmpExtent
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
		domainPtr = &tmpDomain
		cdomain = unsafe.Pointer(&tmpDomain[0])
		// Create extent void*
		tmpExtent := extent.(uint64)
		extentPtr = &tmpExtent
		cextent = unsafe.Pointer(&tmpExtent)
	case TILEDB_FLOAT32:
		if domainType != reflect.Float32 {
			domainTypeMatchDatatype = false
			break
		}
		// Create domain void*
		tmpDomain := domain.([]float32)
		domainPtr = &tmpDomain
		cdomain = unsafe.Pointer(&tmpDomain[0])
		// Create extent void*
		tmpExtent := extent.(float32)
		extentPtr = &tmpExtent
		cextent = unsafe.Pointer(&tmpExtent)
	case TILEDB_FLOAT64:
		if domainType != reflect.Float64 {
			domainTypeMatchDatatype = false
			break
		}
		// Create domain void*
		tmpDomain := domain.([]float64)
		domainPtr = &tmpDomain
		cdomain = unsafe.Pointer(&tmpDomain[0])
		// Create extent void*
		tmpExtent := extent.(float64)
		extentPtr = &tmpExtent
		cextent = unsafe.Pointer(&tmpExtent)
	case TILEDB_BOOL:
		if domainType != reflect.Bool {
			domainTypeMatchDatatype = false
			break
		}
		// Create domain void*
		tmpDomain := domain.([]bool)
		domainPtr = &tmpDomain
		cdomain = unsafe.Pointer(&tmpDomain[0])
		// Create extent void*
		tmpExtent := extent.(bool)
		extentPtr = &tmpExtent
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
	freeOnGC(&dimension)

	return &dimension, nil
}

// NewStringDimension allocates a new string dimension.
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
	freeOnGC(&dimension)

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

// Context exposes the internal TileDB context used to initialize the dimension.
func (d *Dimension) Context() *Context {
	return d.context
}

// SetFilterList sets the dimension filterList.
func (d *Dimension) SetFilterList(filterlist *FilterList) error {
	ret := C.tiledb_dimension_set_filter_list(d.context.tiledbContext, d.tiledbDimension, filterlist.tiledbFilterList)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error setting tiledb dimension filter list: %s", d.context.LastError())
	}
	return nil
}

// FilterList returns a copy of the filter list for attribute.
func (d *Dimension) FilterList() (*FilterList, error) {
	filterList := FilterList{context: d.context}
	ret := C.tiledb_dimension_get_filter_list(d.context.tiledbContext, d.tiledbDimension, &filterList.tiledbFilterList)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error getting tiledb dimension filter list: %s", d.context.LastError())
	}
	freeOnGC(&filterList)

	return &filterList, nil
}

// SetCellValNum sets the number of values per cell for a dimension.
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

// CellValNum returns the number of values of one cell on this attribute.
// For variable-sized attributes returns TILEDB_VAR_NUM.
func (d *Dimension) CellValNum() (uint32, error) {
	var cellValNum C.uint32_t
	ret := C.tiledb_dimension_get_cell_val_num(d.context.tiledbContext, d.tiledbDimension, &cellValNum)
	if ret != C.TILEDB_OK {
		return 0, fmt.Errorf("Error getting tiledb dimension cell val num: %s", d.context.LastError())
	}

	return uint32(cellValNum), nil
}

// Name returns the name of the dimension.
func (d *Dimension) Name() (string, error) {
	var cName *C.char
	ret := C.tiledb_dimension_get_name(d.context.tiledbContext, d.tiledbDimension, &cName)
	if ret != C.TILEDB_OK {
		return "", fmt.Errorf("Error getting tiledb dimension name: %s", d.context.LastError())
	}

	return C.GoString(cName), nil
}

// Type returns the type of the dimension.
func (d *Dimension) Type() (Datatype, error) {
	var cType C.tiledb_datatype_t
	ret := C.tiledb_dimension_get_type(d.context.tiledbContext, d.tiledbDimension, &cType)
	if ret != C.TILEDB_OK {
		return 0, fmt.Errorf("Error getting tiledb dimension type: %s", d.context.LastError())
	}

	return Datatype(cType), nil
}

// Domain returns the dimension's domain.
func (d *Dimension) Domain() (interface{}, error) {
	datatype, err := d.Type()
	if err != nil {
		return nil, err
	}

	switch datatype {
	case TILEDB_INT8:
		return domainInternal[int8](d)
	case TILEDB_INT16:
		return domainInternal[int16](d)
	case TILEDB_INT32:
		return domainInternal[int32](d)
	case TILEDB_INT64:
		return domainInternal[int64](d)
	case TILEDB_UINT8:
		return domainInternal[uint8](d)
	case TILEDB_UINT16:
		return domainInternal[uint16](d)
	case TILEDB_UINT32:
		return domainInternal[uint32](d)
	case TILEDB_UINT64:
		return domainInternal[uint64](d)
	case TILEDB_FLOAT32:
		return domainInternal[float32](d)
	case TILEDB_FLOAT64:
		return domainInternal[float64](d)
	case TILEDB_BOOL:
		// Ensure that our booleans are in canonical true/false form in case they're
		// a value other than 0 or 1.
		asUints, err := domainInternal[uint8](d)
		if err != nil {
			return nil, err
		}
		return []bool{asUints[0] != 0, asUints[1] != 0}, nil
	case TILEDB_STRING_ASCII:
		return nil, nil
	}
	return nil, fmt.Errorf("unrecognized domain type: %d", datatype)
}

func domainInternal[T any](d *Dimension) ([]T, error) {
	// tiledb_dimension_get_domain writes *a pointer to the memory it owns*
	// into cDomain, so we need to ensure that the dimension stays alive for
	// the entire duration of this call.
	defer runtime.KeepAlive(d)
	var cDomain unsafe.Pointer
	ret := C.tiledb_dimension_get_domain(d.context.tiledbContext, d.tiledbDimension, &cDomain)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("could not get tiledb dimension's domain: %w", d.context.LastError())
	}
	asArray := (*[2]T)(cDomain)
	return []T{asArray[0], asArray[1]}, nil
}

// Extent returns the dimension's extent.
func (d *Dimension) Extent() (interface{}, error) {
	datatype, err := d.Type()
	if err != nil {
		return nil, err
	}
	switch datatype {
	case TILEDB_INT8:
		return extentInternal[int8](d)
	case TILEDB_INT16:
		return extentInternal[int16](d)
	case TILEDB_INT32:
		return extentInternal[int32](d)
	case TILEDB_INT64:
		return extentInternal[int64](d)
	case TILEDB_UINT8:
		return extentInternal[uint8](d)
	case TILEDB_UINT16:
		return extentInternal[uint16](d)
	case TILEDB_UINT32:
		return extentInternal[uint32](d)
	case TILEDB_UINT64:
		return extentInternal[uint64](d)
	case TILEDB_FLOAT32:
		return extentInternal[float32](d)
	case TILEDB_FLOAT64:
		return extentInternal[float64](d)
	case TILEDB_BOOL:
		xt, err := extentInternal[uint8](d)
		return xt != 0, err
	case TILEDB_STRING_ASCII:
		return nil, nil
	}
	return nil, fmt.Errorf("unrecognized extent type: %d", datatype)
}

func extentInternal[T any](d *Dimension) (T, error) {
	// As in domainInternal, tiledb_dimension_get_tile_extent writes a pointer
	// to memory it owns into cExtent. Ensure this Dimension stays alive.
	defer runtime.KeepAlive(d)
	var cExtent unsafe.Pointer
	var output T
	cRet := C.tiledb_dimension_get_tile_extent(d.context.tiledbContext, d.tiledbDimension, &cExtent)
	if cRet != C.TILEDB_OK {
		return output, fmt.Errorf("could not get TileDB dimension's extent: %w", d.context.LastError())
	}
	output = *(*T)(cExtent)
	return output, nil
}

// DumpSTDOUT dumps the dimension in ASCII format to stdout.
func (d *Dimension) DumpSTDOUT() error {
	ret := C.tiledb_dimension_dump(d.context.tiledbContext, d.tiledbDimension, C.stdout)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error dumping dimension to stdout: %s", d.context.LastError())
	}
	return nil
}

// Dump dumps the dimension in ASCII format to the given path.
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
