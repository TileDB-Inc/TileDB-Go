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
	"unsafe"
)

/*
Array struct representing a TileDB array object.

An Array object represents array data in TileDB at some persisted location,
e.g. on disk, in an S3 bucket, etc. Once an array has been opened for reading
or writing, interact with the data through Query objects.
*/
type Array struct {
	tiledbArray *C.tiledb_array_t
	context     *Context
	uri         string
}

// NewArray alloc a new array
func NewArray(ctx *Context, uri string) (*Array, error) {
	curi := C.CString(uri)
	defer C.free(unsafe.Pointer(curi))
	array := Array{context: ctx, uri: uri}
	ret := C.tiledb_array_alloc(array.context.tiledbContext, curi, &array.tiledbArray)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error creating tiledb array: %s", array.context.LastError())
	}

	// Set finalizer for free C pointer on gc
	runtime.SetFinalizer(&array, func(array *Array) {
		array.Free()
	})

	return &array, nil
}

// Free tiledb_array_t that was allocated on heap in c
func (a *Array) Free() {
	if a.tiledbArray != nil {
		a.Close()
		C.tiledb_array_free(&a.tiledbArray)
	}
}

/*
Open the array. The array is opened using a query type as input.
This is to indicate that queries created for this Array object will inherit
the query type. In other words, Array objects are opened to receive only one
type of queries. They can always be closed and be re-opened with another query
type. Also there may be many different Array objects created and opened with
different query types. For instance, one may create and open an array object
array_read for reads and another one array_write for writes, and interleave
creation and submission of queries for both these array objects.
*/
func (a *Array) Open(queryType QueryType) error {
	ret := C.tiledb_array_open(a.context.tiledbContext, a.tiledbArray, C.tiledb_query_type_t(queryType))
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error opening tiledb array for querying: %s", a.context.LastError())
	}
	return nil
}

/*
Reopen the array (the array must be already open). This is useful when the
array got updated after it got opened and the Array object got created.
To sync-up with the updates, the user must either close the array and open
with open(), or just use reopen() without closing. This function will be
generally faster than the former alternative.
*/
func (a *Array) Reopen() error {
	ret := C.tiledb_array_reopen(a.context.tiledbContext, a.tiledbArray)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error reopening tiledb array for querying: %s", a.context.LastError())
	}
	return nil
}

// Close a tiledb array, this is called on garbage collection automatically
func (a *Array) Close() error {
	ret := C.tiledb_array_close(a.context.tiledbContext, a.tiledbArray)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error closing tiledb array for querying: %s", a.context.LastError())
	}
	return nil
}

// Create a new TileDB array given an input schema.
func (a *Array) Create(arraySchema *ArraySchema) error {
	curi := C.CString(a.uri)
	defer C.free(unsafe.Pointer(curi))
	ret := C.tiledb_array_create(a.context.tiledbContext, curi, arraySchema.tiledbArraySchema)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error creating tiledb array: %s", a.context.LastError())
	}
	return nil
}

// Consolidate Consolidates the fragments of an array into a single fragment.
// You must first finalize all queries to the array before consolidation can
// begin (as consolidation temporarily acquires an exclusive lock on the array).
func (a *Array) Consolidate() error {
	curi := C.CString(a.uri)
	defer C.free(unsafe.Pointer(curi))
	ret := C.tiledb_array_consolidate(a.context.tiledbContext, curi)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error consolidating tiledb array: %s", a.context.LastError())
	}
	return nil
}

// Schema returns the ArraySchema for the array
func (a *Array) Schema() (*ArraySchema, error) {
	arraySchema := ArraySchema{context: a.context}
	ret := C.tiledb_array_get_schema(a.context.tiledbContext, a.tiledbArray, &arraySchema.tiledbArraySchema)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error getting schema for tiledb array: %s", a.context.LastError())
	}
	return &arraySchema, nil
}

// QueryType return the current query type of an open array
func (a *Array) QueryType() (QueryType, error) {
	var queryType C.tiledb_query_type_t
	ret := C.tiledb_array_get_query_type(a.context.tiledbContext, a.tiledbArray, &queryType)
	if ret != C.TILEDB_OK {
		return -1, fmt.Errorf("Error getting QueryType for tiledb array: %s", a.context.LastError())
	}
	return QueryType(queryType), nil
}

// NonEmptyDomain retrieves the non-empty domain from an array
// This returns the bounding coordinates for each dimension
func (a *Array) NonEmptyDomain() ([]map[string]interface{}, bool, error) {
	nonEmptyDomains := make([]map[string]interface{}, 1)
	schema, err := a.Schema()
	if err != nil {
		return nil, false, err
	}

	domain, err := schema.Domain()
	if err != nil {
		return nil, false, err
	}

	domainType, err := domain.Type()
	if err != nil {
		return nil, false, err
	}

	ndims, err := domain.NDim()
	if err != nil {
		return nil, false, err
	}

	var ret C.int
	var isEmpty C.int
	switch domainType {

	case TILEDB_INT8:
		tmpDomain := make([]int8, 2*ndims)
		ret = C.tiledb_array_get_non_empty_domain(a.context.tiledbContext, a.tiledbArray, unsafe.Pointer(&tmpDomain[0]), &isEmpty)
		if ret != C.TILEDB_OK {
			return nil, false, fmt.Errorf("Error in getting non empty domain for array: %s", a.context.LastError())
		}
		if isEmpty == 0 {
			for i := uint(0); i < ndims; i++ {
				dimension, err := domain.DimensionFromIndex(i)
				if err != nil {
					return nil, false, err
				}

				name, err := dimension.Name()
				if err != nil {
					return nil, false, err
				}
				tmpMap := map[string]interface{}{name: []int8{tmpDomain[i*2], tmpDomain[(i*2)+1]}}
				nonEmptyDomains = append(nonEmptyDomains, tmpMap)
			}
		}
	case TILEDB_INT16:
		tmpDomain := make([]int16, 2*ndims)
		ret = C.tiledb_array_get_non_empty_domain(a.context.tiledbContext, a.tiledbArray, unsafe.Pointer(&tmpDomain[0]), &isEmpty)
		if ret != C.TILEDB_OK {
			return nil, false, fmt.Errorf("Error in getting non empty domain for array: %s", a.context.LastError())
		}
		if isEmpty == 0 {
			for i := uint(0); i < ndims; i++ {
				dimension, err := domain.DimensionFromIndex(i)
				if err != nil {
					return nil, false, err
				}

				name, err := dimension.Name()
				if err != nil {
					return nil, false, err
				}
				tmpMap := map[string]interface{}{name: []int16{tmpDomain[i*2], tmpDomain[(i*2)+1]}}
				nonEmptyDomains = append(nonEmptyDomains, tmpMap)
			}
		}
	case TILEDB_INT32:
		tmpDomain := make([]int32, 2*ndims)
		ret = C.tiledb_array_get_non_empty_domain(a.context.tiledbContext, a.tiledbArray, unsafe.Pointer(&tmpDomain[0]), &isEmpty)
		if ret != C.TILEDB_OK {
			return nil, false, fmt.Errorf("Error in getting non empty domain for array: %s", a.context.LastError())
		}
		if isEmpty == 0 {
			for i := uint(0); i < ndims; i++ {
				dimension, err := domain.DimensionFromIndex(i)
				if err != nil {
					return nil, false, err
				}

				name, err := dimension.Name()
				if err != nil {
					return nil, false, err
				}
				tmpMap := map[string]interface{}{name: []int32{tmpDomain[i*2], tmpDomain[(i*2)+1]}}
				nonEmptyDomains = append(nonEmptyDomains, tmpMap)
			}
		}
	case TILEDB_INT64:
		tmpDomain := make([]int64, 2*ndims)
		ret = C.tiledb_array_get_non_empty_domain(a.context.tiledbContext, a.tiledbArray, unsafe.Pointer(&tmpDomain[0]), &isEmpty)
		if ret != C.TILEDB_OK {
			return nil, false, fmt.Errorf("Error in getting non empty domain for array: %s", a.context.LastError())
		}
		if isEmpty == 0 {
			for i := uint(0); i < ndims; i++ {
				dimension, err := domain.DimensionFromIndex(i)
				if err != nil {
					return nil, false, err
				}

				name, err := dimension.Name()
				if err != nil {
					return nil, false, err
				}
				tmpMap := map[string]interface{}{name: []int64{tmpDomain[i*2], tmpDomain[(i*2)+1]}}
				nonEmptyDomains = append(nonEmptyDomains, tmpMap)
			}
		}
	case TILEDB_UINT8:
		tmpDomain := make([]uint8, 2*ndims)
		ret = C.tiledb_array_get_non_empty_domain(a.context.tiledbContext, a.tiledbArray, unsafe.Pointer(&tmpDomain[0]), &isEmpty)
		if ret != C.TILEDB_OK {
			return nil, false, fmt.Errorf("Error in getting non empty domain for array: %s", a.context.LastError())
		}
		if isEmpty == 0 {
			for i := uint(0); i < ndims; i++ {
				dimension, err := domain.DimensionFromIndex(i)
				if err != nil {
					return nil, false, err
				}

				name, err := dimension.Name()
				if err != nil {
					return nil, false, err
				}
				tmpMap := map[string]interface{}{name: []uint8{tmpDomain[i*2], tmpDomain[(i*2)+1]}}
				nonEmptyDomains = append(nonEmptyDomains, tmpMap)
			}
		}
	case TILEDB_UINT16:
		tmpDomain := make([]uint16, 2*ndims)
		ret = C.tiledb_array_get_non_empty_domain(a.context.tiledbContext, a.tiledbArray, unsafe.Pointer(&tmpDomain[0]), &isEmpty)
		if ret != C.TILEDB_OK {
			return nil, false, fmt.Errorf("Error in getting non empty domain for array: %s", a.context.LastError())
		}
		if isEmpty == 0 {
			for i := uint(0); i < ndims; i++ {
				dimension, err := domain.DimensionFromIndex(i)
				if err != nil {
					return nil, false, err
				}

				name, err := dimension.Name()
				if err != nil {
					return nil, false, err
				}
				tmpMap := map[string]interface{}{name: []uint16{tmpDomain[i*2], tmpDomain[(i*2)+1]}}
				nonEmptyDomains = append(nonEmptyDomains, tmpMap)
			}
		}
	case TILEDB_UINT32:
		tmpDomain := make([]uint32, 2*ndims)
		ret = C.tiledb_array_get_non_empty_domain(a.context.tiledbContext, a.tiledbArray, unsafe.Pointer(&tmpDomain[0]), &isEmpty)
		if ret != C.TILEDB_OK {
			return nil, false, fmt.Errorf("Error in getting non empty domain for array: %s", a.context.LastError())
		}
		if isEmpty == 0 {
			for i := uint(0); i < ndims; i++ {
				dimension, err := domain.DimensionFromIndex(i)
				if err != nil {
					return nil, false, err
				}

				name, err := dimension.Name()
				if err != nil {
					return nil, false, err
				}
				tmpMap := map[string]interface{}{name: []uint32{tmpDomain[i*2], tmpDomain[(i*2)+1]}}
				nonEmptyDomains = append(nonEmptyDomains, tmpMap)
			}
		}
	case TILEDB_UINT64:
		tmpDomain := make([]uint64, 2*ndims)
		ret = C.tiledb_array_get_non_empty_domain(a.context.tiledbContext, a.tiledbArray, unsafe.Pointer(&tmpDomain[0]), &isEmpty)
		if ret != C.TILEDB_OK {
			return nil, false, fmt.Errorf("Error in getting non empty domain for array: %s", a.context.LastError())
		}
		if isEmpty == 0 {
			for i := uint(0); i < ndims; i++ {
				dimension, err := domain.DimensionFromIndex(i)
				if err != nil {
					return nil, false, err
				}

				name, err := dimension.Name()
				if err != nil {
					return nil, false, err
				}
				tmpMap := map[string]interface{}{name: []uint64{tmpDomain[i*2], tmpDomain[(i*2)+1]}}
				nonEmptyDomains = append(nonEmptyDomains, tmpMap)
			}
		}
	case TILEDB_FLOAT32:
		tmpDomain := make([]float32, 2*ndims)
		ret = C.tiledb_array_get_non_empty_domain(a.context.tiledbContext, a.tiledbArray, unsafe.Pointer(&tmpDomain[0]), &isEmpty)
		if ret != C.TILEDB_OK {
			return nil, false, fmt.Errorf("Error in getting non empty domain for array: %s", a.context.LastError())
		}
		if isEmpty == 0 {
			for i := uint(0); i < ndims; i++ {
				dimension, err := domain.DimensionFromIndex(i)
				if err != nil {
					return nil, false, err
				}

				name, err := dimension.Name()
				if err != nil {
					return nil, false, err
				}
				tmpMap := map[string]interface{}{name: []float32{tmpDomain[i*2], tmpDomain[(i*2)+1]}}
				nonEmptyDomains = append(nonEmptyDomains, tmpMap)
			}
		}
	case TILEDB_FLOAT64:
		tmpDomain := make([]float64, 2*ndims)
		ret = C.tiledb_array_get_non_empty_domain(a.context.tiledbContext, a.tiledbArray, unsafe.Pointer(&tmpDomain[0]), &isEmpty)
		if ret != C.TILEDB_OK {
			return nil, false, fmt.Errorf("Error in getting non empty domain for array: %s", a.context.LastError())
		}
		if isEmpty == 0 {
			for i := uint(0); i < ndims; i++ {
				dimension, err := domain.DimensionFromIndex(i)
				if err != nil {
					return nil, false, err
				}

				name, err := dimension.Name()
				if err != nil {
					return nil, false, err
				}
				tmpMap := map[string]interface{}{name: []float64{tmpDomain[i*2], tmpDomain[(i*2)+1]}}
				nonEmptyDomains = append(nonEmptyDomains, tmpMap)
			}
		}
	}
	return nonEmptyDomains, isEmpty == 1, nil
}

// MaxBufferSize computes the upper bound on the buffer size (in bytes)
// required for a read query for a given fixed attribute and subarray
func (a *Array) MaxBufferSize(attributeName string, subarray interface{}) (uint64, error) {
	// Get Schema
	schema, err := a.Schema()
	if err != nil {
		return 0, err
	}

	// Get domain from schema
	domain, err := schema.Domain()
	if err != nil {
		return 0, err
	}

	// Get domain type to switch on
	domainType, err := domain.Type()
	if err != nil {
		return 0, err
	}

	cAttributeName := C.CString(attributeName)
	defer C.free(unsafe.Pointer(cAttributeName))

	var bufferSize C.uint64_t
	var ret C.int
	// Switch on domain type to cast subarray to proper type
	switch domainType {
	case TILEDB_INT8:
		tmpSubArray := subarray.([]int8)
		ret = C.tiledb_array_max_buffer_size(a.context.tiledbContext, a.tiledbArray, cAttributeName, unsafe.Pointer(&tmpSubArray[0]), &bufferSize)
	case TILEDB_INT16:
		tmpSubArray := subarray.([]int16)
		ret = C.tiledb_array_max_buffer_size(a.context.tiledbContext, a.tiledbArray, cAttributeName, unsafe.Pointer(&tmpSubArray[0]), &bufferSize)
	case TILEDB_INT32:
		tmpSubArray := subarray.([]int32)
		ret = C.tiledb_array_max_buffer_size(a.context.tiledbContext, a.tiledbArray, cAttributeName, unsafe.Pointer(&tmpSubArray[0]), &bufferSize)
	case TILEDB_INT64:
		tmpSubArray := subarray.([]int64)
		ret = C.tiledb_array_max_buffer_size(a.context.tiledbContext, a.tiledbArray, cAttributeName, unsafe.Pointer(&tmpSubArray[0]), &bufferSize)
	case TILEDB_UINT8:
		tmpSubArray := subarray.([]uint8)
		ret = C.tiledb_array_max_buffer_size(a.context.tiledbContext, a.tiledbArray, cAttributeName, unsafe.Pointer(&tmpSubArray[0]), &bufferSize)
	case TILEDB_UINT16:
		tmpSubArray := subarray.([]uint16)
		ret = C.tiledb_array_max_buffer_size(a.context.tiledbContext, a.tiledbArray, cAttributeName, unsafe.Pointer(&tmpSubArray[0]), &bufferSize)
	case TILEDB_UINT32:
		tmpSubArray := subarray.([]uint32)
		ret = C.tiledb_array_max_buffer_size(a.context.tiledbContext, a.tiledbArray, cAttributeName, unsafe.Pointer(&tmpSubArray[0]), &bufferSize)
	case TILEDB_UINT64:
		tmpSubArray := subarray.([]uint64)
		ret = C.tiledb_array_max_buffer_size(a.context.tiledbContext, a.tiledbArray, cAttributeName, unsafe.Pointer(&tmpSubArray[0]), &bufferSize)
	case TILEDB_FLOAT32:
		tmpSubArray := subarray.([]float32)
		ret = C.tiledb_array_max_buffer_size(a.context.tiledbContext, a.tiledbArray, cAttributeName, unsafe.Pointer(&tmpSubArray[0]), &bufferSize)
	case TILEDB_FLOAT64:
		tmpSubArray := subarray.([]float64)
		ret = C.tiledb_array_max_buffer_size(a.context.tiledbContext, a.tiledbArray, cAttributeName, unsafe.Pointer(&tmpSubArray[0]), &bufferSize)
	}
	if ret != C.TILEDB_OK {
		return 0, fmt.Errorf("Error in getting max buffer size for array: %s", a.context.LastError())
	}

	return uint64(bufferSize), nil
}

// MaxBufferSizeVar computes the upper bound on the buffer size (in bytes)
// required for a read query for a given variable sized attribute and subarray
func (a *Array) MaxBufferSizeVar(attributeName string, subarray interface{}) (uint64, uint64, error) {
	// Get Schema
	schema, err := a.Schema()
	if err != nil {
		return 0, 0, err
	}

	// Get domain from schema
	domain, err := schema.Domain()
	if err != nil {
		return 0, 0, err
	}

	// Get domain type to switch on
	domainType, err := domain.Type()
	if err != nil {
		return 0, 0, err
	}

	cAttributeName := C.CString(attributeName)
	defer C.free(unsafe.Pointer(cAttributeName))

	var bufferValSize C.uint64_t
	var bufferOffSize C.uint64_t
	var ret C.int
	// Switch on domain type to cast subarray to proper type
	switch domainType {
	case TILEDB_INT8:
		tmpSubArray := subarray.([]int8)
		ret = C.tiledb_array_max_buffer_size_var(a.context.tiledbContext, a.tiledbArray, cAttributeName, unsafe.Pointer(&tmpSubArray[0]), &bufferOffSize, &bufferValSize)
	case TILEDB_INT16:
		tmpSubArray := subarray.([]int16)
		ret = C.tiledb_array_max_buffer_size_var(a.context.tiledbContext, a.tiledbArray, cAttributeName, unsafe.Pointer(&tmpSubArray[0]), &bufferOffSize, &bufferValSize)
	case TILEDB_INT32:
		tmpSubArray := subarray.([]int32)
		ret = C.tiledb_array_max_buffer_size_var(a.context.tiledbContext, a.tiledbArray, cAttributeName, unsafe.Pointer(&tmpSubArray[0]), &bufferOffSize, &bufferValSize)
	case TILEDB_INT64:
		tmpSubArray := subarray.([]int64)
		ret = C.tiledb_array_max_buffer_size_var(a.context.tiledbContext, a.tiledbArray, cAttributeName, unsafe.Pointer(&tmpSubArray[0]), &bufferOffSize, &bufferValSize)
	case TILEDB_UINT8:
		tmpSubArray := subarray.([]uint8)
		ret = C.tiledb_array_max_buffer_size_var(a.context.tiledbContext, a.tiledbArray, cAttributeName, unsafe.Pointer(&tmpSubArray[0]), &bufferOffSize, &bufferValSize)
	case TILEDB_UINT16:
		tmpSubArray := subarray.([]uint16)
		ret = C.tiledb_array_max_buffer_size_var(a.context.tiledbContext, a.tiledbArray, cAttributeName, unsafe.Pointer(&tmpSubArray[0]), &bufferOffSize, &bufferValSize)
	case TILEDB_UINT32:
		tmpSubArray := subarray.([]uint32)
		ret = C.tiledb_array_max_buffer_size_var(a.context.tiledbContext, a.tiledbArray, cAttributeName, unsafe.Pointer(&tmpSubArray[0]), &bufferOffSize, &bufferValSize)
	case TILEDB_UINT64:
		tmpSubArray := subarray.([]uint64)
		ret = C.tiledb_array_max_buffer_size_var(a.context.tiledbContext, a.tiledbArray, cAttributeName, unsafe.Pointer(&tmpSubArray[0]), &bufferOffSize, &bufferValSize)
	case TILEDB_FLOAT32:
		tmpSubArray := subarray.([]float32)
		ret = C.tiledb_array_max_buffer_size_var(a.context.tiledbContext, a.tiledbArray, cAttributeName, unsafe.Pointer(&tmpSubArray[0]), &bufferOffSize, &bufferValSize)
	case TILEDB_FLOAT64:
		tmpSubArray := subarray.([]float64)
		ret = C.tiledb_array_max_buffer_size_var(a.context.tiledbContext, a.tiledbArray, cAttributeName, unsafe.Pointer(&tmpSubArray[0]), &bufferOffSize, &bufferValSize)
	}
	if ret != C.TILEDB_OK {
		return 0, 0, fmt.Errorf("Error in getting max buffer size variable for array: %s", a.context.LastError())
	}

	return uint64(bufferOffSize), uint64(bufferValSize), nil
}

/*
MaxBufferElements compute an upper bound on the buffer elements needed to
read a subarray.
Returns A map of attribute name (including TILEDB_COORDS) to the maximum
number of elements that can be read in the given subarray. For each attribute,
a pair of numbers are returned. The first, for variable-length attributes, is
the maximum number of offsets for that attribute in the given subarray. For
fixed-length attributes and coordinates, the first is always 0. The second
is the maximum number of elements for that attribute in the given subarray.
*/
func (a *Array) MaxBufferElements(subarray interface{}) (map[string][2]uint64, error) {
	// Build map
	ret := make(map[string][2]uint64, 0)
	// Get schema
	schema, err := a.Schema()
	if err != nil {
		return nil, fmt.Errorf("Error getting MaxBufferElements for array: %s", err)
	}

	// Get the attribute number
	attrNum, err := schema.AttributeNum()
	if err != nil {
		return nil, fmt.Errorf("Error getting MaxBufferElements for array: %s", err)
	}

	// Loop through each attribute
	for i := uint(0); i < attrNum; i++ {
		attribute, err := schema.AttributeFromIndex(i)
		if err != nil {
			return nil, fmt.Errorf("Error getting MaxBufferElements for array: %s", err)
		}
		// Check if attribute is variable attribute or not
		cellValNum, err := attribute.CellValNum()
		if err != nil {
			return nil, fmt.Errorf("Error getting MaxBufferElements for array: %s", err)
		}

		// Get datatype size to convert byte lengths to needed buffer sizes
		dataType, err := attribute.Type()
		dataTypeSize := uint64(C.tiledb_datatype_size(C.tiledb_datatype_t(dataType)))

		// Get attribute name
		name, err := attribute.Name()
		if err != nil {
			return nil, fmt.Errorf("Error getting MaxBufferElements for array: %s", err)
		}

		if cellValNum == TILEDB_VAR_NUM {
			bufferOffsetSize, bufferValSize, err := a.MaxBufferSizeVar(name, subarray)
			if err != nil {
				return nil, fmt.Errorf("Error getting MaxBufferElements for array: %s", err)
			}
			// Set sizes for attribute in return map
			ret[name] = [2]uint64{
				bufferOffsetSize / uint64(C.TILEDB_OFFSET_SIZE),
				bufferValSize / dataTypeSize}
			if err != nil {
				return nil, fmt.Errorf("Error getting MaxBufferElements for array: %s", err)
			}
		} else {
			bufferValSize, err := a.MaxBufferSize(name, subarray)
			if err != nil {
				return nil, fmt.Errorf("Error getting MaxBufferElements for array: %s", err)
			}
			ret[name] = [2]uint64{0, bufferValSize / dataTypeSize}
		}
	}

	return ret, nil
}
