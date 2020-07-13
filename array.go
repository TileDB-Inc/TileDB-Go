package tiledb

/*
#cgo LDFLAGS: -ltiledb
#cgo linux LDFLAGS: -ldl
#include <tiledb/tiledb.h>
#include <stdlib.h>
*/
import "C"

import (
	"encoding/json"
	"fmt"
	"reflect"
	"runtime"
	"strconv"
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

// ArrayMetadata defines metadata for the array
type ArrayMetadata struct {
	Key      string
	KeyLen   uint32
	Datatype Datatype
	ValueNum uint
	Value    interface{}
}

// MarshalJSON implements the Marshaler interface for ArrayMetadata
func (a ArrayMetadata) MarshalJSON() ([]byte, error) {
	return json.Marshal(a.Value)
}

// NonEmptyDomain contains the non empty dimension bounds and dimension name
type NonEmptyDomain struct {
	DimensionName string
	Bounds        interface{}
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
OpenWithKey Opens an encrypted array using the given encryption key.
This function has the same semantics as tiledb_array_open() but is used
for encrypted arrays.

An encrypted array must be opened with this function before queries can
be issued to it.
*/
func (a *Array) OpenWithKey(queryType QueryType, encryptionType EncryptionType, key string) error {
	ckey := unsafe.Pointer(C.CString(key))
	defer C.free(ckey)
	ret := C.tiledb_array_open_with_key(a.context.tiledbContext, a.tiledbArray, C.tiledb_query_type_t(queryType), C.tiledb_encryption_type_t(encryptionType), ckey, C.uint32_t(len(key)))
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error opening tiledb array with key for querying: %s", a.context.LastError())
	}
	return nil
}

/*
OpenAt Similar to tiledb_array_open, but this function takes as input
a timestamp, representing time in milliseconds ellapsed since
1970-01-01 00:00:00 +0000 (UTC). Opening the array at a timestamp provides
a view of the array with all writes/updates that happened at or before
timestamp (i.e., excluding those that occurred after timestamp). This
function is useful to ensure consistency at a potential distributed
setting, where machines need to operate on the same view of the array.
*/
func (a *Array) OpenAt(queryType QueryType, timestamp uint64) error {
	ret := C.tiledb_array_open_at(a.context.tiledbContext, a.tiledbArray, C.tiledb_query_type_t(queryType), C.uint64_t(timestamp))
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error opening tiledb array at %d for querying: %s", timestamp, a.context.LastError())
	}
	return nil
}

/*
OpenAtWithKey Similar to tiledb_array_open_with_key, but this function
takes as input a timestamp, representing time in milliseconds ellapsed
since 1970-01-01 00:00:00 +0000 (UTC). Opening the array at a timestamp
provides a view of the array with all writes/updates that happened at or
before timestamp (i.e., excluding those that occurred after timestamp).
This function is useful to ensure consistency at a potential distributed
setting, where machines need to operate on the same view of the array.
*/
func (a *Array) OpenAtWithKey(queryType QueryType, encryptionType EncryptionType, key string, timestamp uint64) error {
	ckey := unsafe.Pointer(C.CString(key))
	defer C.free(ckey)
	ret := C.tiledb_array_open_at_with_key(a.context.tiledbContext, a.tiledbArray, C.tiledb_query_type_t(queryType), C.tiledb_encryption_type_t(encryptionType), ckey, C.uint32_t(len(key)), C.uint64_t(timestamp))
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error opening tiledb array with key at %d for querying: %s", timestamp, a.context.LastError())
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

// CreateWithKey a new TileDB array given an input schema.
func (a *Array) CreateWithKey(arraySchema *ArraySchema, encryptionType EncryptionType, key string) error {
	ckey := unsafe.Pointer(C.CString(key))
	defer C.free(ckey)
	curi := C.CString(a.uri)
	defer C.free(unsafe.Pointer(curi))
	ret := C.tiledb_array_create_with_key(a.context.tiledbContext, curi, arraySchema.tiledbArraySchema, C.tiledb_encryption_type_t(encryptionType), ckey, C.uint32_t(len(key)))
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error creating tiledb array with key: %s", a.context.LastError())
	}
	return nil
}

// Consolidate Consolidates the fragments of an array into a single fragment.
// You must first finalize all queries to the array before consolidation can
// begin (as consolidation temporarily acquires an exclusive lock on the array).
func (a *Array) Consolidate(config *Config) error {
	if config == nil {
		return fmt.Errorf("Config must not be nil for Consolidate")
	}

	curi := C.CString(a.uri)
	defer C.free(unsafe.Pointer(curi))
	ret := C.tiledb_array_consolidate(a.context.tiledbContext, curi, config.tiledbConfig)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error consolidating tiledb array: %s", a.context.LastError())
	}
	return nil
}

// ConsolidateWithKey Consolidates the fragments of an encrypted array
// into a single fragment.
// You must first finalize all queries to the array before consolidation can
// begin (as consolidation temporarily acquires an exclusive lock on the array).
func (a *Array) ConsolidateWithKey(encryptionType EncryptionType, key string, config *Config) error {
	if config == nil {
		return fmt.Errorf("Config must not be nil for ConsolidateWithKey")
	}

	ckey := unsafe.Pointer(C.CString(key))
	defer C.free(ckey)
	curi := C.CString(a.uri)
	defer C.free(unsafe.Pointer(curi))

	ret := C.tiledb_array_consolidate_with_key(a.context.tiledbContext, curi, C.tiledb_encryption_type_t(encryptionType), ckey, C.uint32_t(len(key)), config.tiledbConfig)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error consolidating tiledb with key array: %s", a.context.LastError())
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

// getNonEmptyDomainForDim creates a NonEmptyDomain from a generic dimension-typed slice
func getNonEmptyDomainForDim(dimension *Dimension, dimensionSlice interface{}) (*NonEmptyDomain, error) {
	dimensionType, err := dimension.Type()
	if err != nil {
		return nil, err
	}

	name, err := dimension.Name()
	if err != nil {
		return nil, err
	}

	var nonEmptyDomain NonEmptyDomain
	switch dimensionType {
	case TILEDB_INT8:
		tmpDimension := dimensionSlice.([]int8)
		nonEmptyDomain = NonEmptyDomain{DimensionName: name, Bounds: []int8{tmpDimension[0], tmpDimension[1]}}
	case TILEDB_INT16:
		tmpDimension := dimensionSlice.([]int16)
		nonEmptyDomain = NonEmptyDomain{DimensionName: name, Bounds: []int16{tmpDimension[0], tmpDimension[1]}}
	case TILEDB_INT32:
		tmpDimension := dimensionSlice.([]int32)
		nonEmptyDomain = NonEmptyDomain{DimensionName: name, Bounds: []int32{tmpDimension[0], tmpDimension[1]}}
	case TILEDB_INT64:
		tmpDimension := dimensionSlice.([]int64)
		nonEmptyDomain = NonEmptyDomain{DimensionName: name, Bounds: []int64{tmpDimension[0], tmpDimension[1]}}
	case TILEDB_UINT8:
		tmpDimension := dimensionSlice.([]uint8)
		nonEmptyDomain = NonEmptyDomain{DimensionName: name, Bounds: []uint8{tmpDimension[0], tmpDimension[1]}}
	case TILEDB_UINT16:
		tmpDimension := dimensionSlice.([]uint16)
		nonEmptyDomain = NonEmptyDomain{DimensionName: name, Bounds: []uint16{tmpDimension[0], tmpDimension[1]}}
	case TILEDB_UINT32:
		tmpDimension := dimensionSlice.([]uint32)
		nonEmptyDomain = NonEmptyDomain{DimensionName: name, Bounds: []uint32{tmpDimension[0], tmpDimension[1]}}
	case TILEDB_UINT64:
		tmpDimension := dimensionSlice.([]uint64)
		nonEmptyDomain = NonEmptyDomain{DimensionName: name, Bounds: []uint64{tmpDimension[0], tmpDimension[1]}}
	case TILEDB_FLOAT32:
		tmpDimension := dimensionSlice.([]float32)
		nonEmptyDomain = NonEmptyDomain{DimensionName: name, Bounds: []float32{tmpDimension[0], tmpDimension[1]}}
	case TILEDB_FLOAT64:
		tmpDimension := dimensionSlice.([]float64)
		nonEmptyDomain = NonEmptyDomain{DimensionName: name, Bounds: []float64{tmpDimension[0], tmpDimension[1]}}
	case TILEDB_STRING_ASCII:
		tmpDimension := dimensionSlice.([]interface{})
		lowBound := tmpDimension[0].([]uint8)
		highBound := tmpDimension[1].([]uint8)
		nonEmptyDomain = NonEmptyDomain{DimensionName: name, Bounds: []string{string(lowBound), string(highBound)}}
	default:
		return nil, fmt.Errorf("error creating non empty domain: unknown dimension type")
	}

	return &nonEmptyDomain, nil
}

// NonEmptyDomain retrieves the non-empty domain from an array
// This returns the bounding coordinates for each dimension
func (a *Array) NonEmptyDomain() ([]NonEmptyDomain, bool, error) {
	schema, err := a.Schema()
	if err != nil {
		return nil, false, err
	}

	domain, err := schema.Domain()
	if err != nil {
		return nil, false, err
	}

	ndims, err := domain.NDim()
	if err != nil {
		return nil, false, err
	}

	isDomainEmpty := true
	nonEmptyDomains := make([]NonEmptyDomain, 0)
	for dimIdx := uint(0); dimIdx < ndims; dimIdx++ {
		dimension, err := domain.DimensionFromIndex(dimIdx)
		if err != nil {
			return nil, false, err
		}

		dimensionType, err := dimension.Type()
		if err != nil {
			return nil, false, err
		}

		tmpDimension, tmpDimensionPtr, err := dimensionType.MakeSlice(uint64(2))
		if err != nil {
			return nil, false, err
		}

		var isEmpty C.int32_t
		ret := C.tiledb_array_get_non_empty_domain_from_index(
			a.context.tiledbContext,
			a.tiledbArray,
			(C.uint32_t)(dimIdx),
			tmpDimensionPtr, &isEmpty)
		if ret != C.TILEDB_OK {
			return nil, false, fmt.Errorf("Error in getting non empty domain for dimension: %s", a.context.LastError())
		}

		if isEmpty == 1 {
			continue
		} else {
			// If at least one domain for a dimension is empty the union of domains is non-empty
			isDomainEmpty = false
			nonEmptyDomain, err := getNonEmptyDomainForDim(dimension, tmpDimension)
			if err != nil {
				return nil, false, err
			}
			nonEmptyDomains = append(nonEmptyDomains, *nonEmptyDomain)
		}
	}

	if isDomainEmpty {
		return nil, isDomainEmpty, nil
	}

	return nonEmptyDomains, isDomainEmpty, nil
}

// NonEmptyDomainVarFromName retrieves the non-empty domain from an array for a
// given var-sized dimension name. Supports only TILEDB_STRING_ASCII type
// Returns the bounding coordinates for the dimension
func (a *Array) NonEmptyDomainVarFromName(dimName string) (*NonEmptyDomain, bool, error) {

	schema, err := a.Schema()
	if err != nil {
		return nil, false, err
	}

	domain, err := schema.Domain()
	if err != nil {
		return nil, false, err
	}

	hasDim, err := domain.HasDimension(dimName)
	if err != nil {
		return nil, false, err
	}

	if !hasDim {
		return nil, false, fmt.Errorf("Dimension: %s was not found in domain", dimName)
	}

	dimension, err := domain.DimensionFromName(dimName)
	if err != nil {
		return nil, false, fmt.Errorf("Could not get dimension: %s", dimName)
	}

	dimType, err := dimension.Type()
	if err != nil {
		return nil, false, err
	}

	cDimName := C.CString(dimName)
	defer C.free(unsafe.Pointer(cDimName))

	var cstartSize C.uint64_t
	var cendSize C.uint64_t

	var isEmpty C.int32_t

	var start interface{}
	var end interface{}
	var cstart unsafe.Pointer
	var cend unsafe.Pointer

	ret := C.tiledb_array_get_non_empty_domain_var_size_from_name(
		a.context.tiledbContext,
		a.tiledbArray,
		cDimName,
		&cstartSize,
		&cendSize,
		&isEmpty)
	if ret != C.TILEDB_OK {
		return nil, false, fmt.Errorf("Error in getting non empty domain size for dimension %s for array: %s", dimName, a.context.LastError())
	}

	if isEmpty == 1 {
		return nil, true, nil
	}

	bounds := make([]interface{}, 0)

	start, cstart, err = dimType.MakeSlice(uint64(cstartSize))
	if err != nil {
		return nil, false, err
	}
	bounds = append(bounds, start)

	end, cend, err = dimType.MakeSlice(uint64(cendSize))
	if err != nil {
		return nil, false, err
	}
	bounds = append(bounds, end)

	ret = C.tiledb_array_get_non_empty_domain_var_from_name(
		a.context.tiledbContext,
		a.tiledbArray,
		cDimName,
		cstart,
		cend,
		&isEmpty)
	if ret != C.TILEDB_OK {
		return nil, false, fmt.Errorf("Error in getting non empty domain for dimension %s for array: %s", dimName, a.context.LastError())
	}

	if isEmpty == 1 {
		return nil, true, nil
	}

	nonEmptyDomain, err := getNonEmptyDomainForDim(dimension, bounds)
	if err != nil {
		return nil, false, err
	}

	return nonEmptyDomain, false, nil
}

// NonEmptyDomainVarFromIndex retrieves the non-empty domain from an array for a
// given var-sized dimension index. Supports only TILEDB_STRING_ASCII type
// Returns the bounding coordinates for the dimension
func (a *Array) NonEmptyDomainVarFromIndex(dimIdx uint) (*NonEmptyDomain, bool, error) {

	schema, err := a.Schema()
	if err != nil {
		return nil, false, err
	}

	domain, err := schema.Domain()
	if err != nil {
		return nil, false, err
	}

	dimension, err := domain.DimensionFromIndex(dimIdx)
	if err != nil {
		return nil, false, fmt.Errorf("Could not get dimension having index: %d", dimIdx)
	}

	dimType, err := dimension.Type()
	if err != nil {
		return nil, false, err
	}

	var cstartSize C.uint64_t
	var cendSize C.uint64_t

	var isEmpty C.int32_t

	var start interface{}
	var end interface{}
	var cstart unsafe.Pointer
	var cend unsafe.Pointer

	ret := C.tiledb_array_get_non_empty_domain_var_size_from_index(
		a.context.tiledbContext,
		a.tiledbArray,
		(C.uint32_t)(dimIdx),
		&cstartSize,
		&cendSize,
		&isEmpty)
	if ret != C.TILEDB_OK {
		return nil, false, fmt.Errorf("Error in getting non empty domain size for dimension %d for array: %s", dimIdx, a.context.LastError())
	}

	if isEmpty == 1 {
		return nil, true, nil
	}

	bounds := make([]interface{}, 0)

	start, cstart, err = dimType.MakeSlice(uint64(cstartSize))
	if err != nil {
		return nil, false, err
	}
	bounds = append(bounds, start)

	end, cend, err = dimType.MakeSlice(uint64(cendSize))
	if err != nil {
		return nil, false, err
	}
	bounds = append(bounds, end)

	ret = C.tiledb_array_get_non_empty_domain_var_from_index(
		a.context.tiledbContext,
		a.tiledbArray,
		(C.uint32_t)(dimIdx),
		cstart,
		cend,
		&isEmpty)
	if ret != C.TILEDB_OK {
		return nil, false, fmt.Errorf("Error in getting non empty domain for dimension index %d for array: %s", dimIdx, a.context.LastError())
	}

	if isEmpty == 1 {
		return nil, true, nil
	}

	nonEmptyDomain, err := getNonEmptyDomainForDim(dimension, bounds)
	if err != nil {
		return nil, false, err
	}

	return nonEmptyDomain, false, nil
}

// NonEmptyDomainFromName retrieves the non-empty domain from an array for a
// given fixed-sized dimension name
// Returns the bounding coordinates for the dimension
func (a *Array) NonEmptyDomainFromName(dimName string) (*NonEmptyDomain, bool, error) {
	schema, err := a.Schema()
	if err != nil {
		return nil, false, err
	}

	domain, err := schema.Domain()
	if err != nil {
		return nil, false, err
	}

	hasDim, err := domain.HasDimension(dimName)
	if err != nil {
		return nil, false, err
	}

	if !hasDim {
		return nil, false, fmt.Errorf("Dimension: %s was not found in domain", dimName)
	}

	dimension, err := domain.DimensionFromName(dimName)
	if err != nil {
		return nil, false, fmt.Errorf("Could not get dimension: %s", dimName)
	}

	dimensionType, err := dimension.Type()
	if err != nil {
		return nil, false, err
	}

	tmpDimension, tmpDimensionPtr, err := dimensionType.MakeSlice(uint64(2))
	if err != nil {
		return nil, false, err
	}

	cDimName := C.CString(dimName)
	defer C.free(unsafe.Pointer(cDimName))

	var isEmpty C.int32_t
	ret := C.tiledb_array_get_non_empty_domain_from_name(
		a.context.tiledbContext,
		a.tiledbArray,
		cDimName,
		tmpDimensionPtr, &isEmpty)
	if ret != C.TILEDB_OK {
		return nil, false, fmt.Errorf("Error in getting non empty domain for dimension: %s", a.context.LastError())
	}

	if isEmpty == 1 {
		return nil, true, nil
	}
	// If at least one domain for a dimension is empty the union of domains is non-empty
	nonEmptyDomain, err := getNonEmptyDomainForDim(dimension, tmpDimension)
	if err != nil {
		return nil, false, err
	}

	return nonEmptyDomain, false, nil
}

// NonEmptyDomainFromIndex retrieves the non-empty domain from an array for a
// given fixed-sized dimension index.
// Returns the bounding coordinates for the dimension
func (a *Array) NonEmptyDomainFromIndex(dimIdx uint) (*NonEmptyDomain, bool, error) {
	schema, err := a.Schema()
	if err != nil {
		return nil, false, err
	}

	domain, err := schema.Domain()
	if err != nil {
		return nil, false, err
	}

	dimension, err := domain.DimensionFromIndex(dimIdx)
	if err != nil {
		return nil, false, fmt.Errorf("Could not get dimension: %d", dimIdx)
	}

	dimensionType, err := dimension.Type()
	if err != nil {
		return nil, false, err
	}

	tmpDimension, tmpDimensionPtr, err := dimensionType.MakeSlice(uint64(2))
	if err != nil {
		return nil, false, err
	}

	var isEmpty C.int32_t
	ret := C.tiledb_array_get_non_empty_domain_from_index(
		a.context.tiledbContext,
		a.tiledbArray,
		(C.uint32_t)(dimIdx),
		tmpDimensionPtr, &isEmpty)
	if ret != C.TILEDB_OK {
		return nil, false, fmt.Errorf("Error in getting non empty domain for dimension: %s", a.context.LastError())
	}

	if isEmpty == 1 {
		return nil, true, nil
	}
	// If at least one domain for a dimension is empty the union of domains is non-empty
	nonEmptyDomain, err := getNonEmptyDomainForDim(dimension, tmpDimension)
	if err != nil {
		return nil, false, err
	}

	return nonEmptyDomain, false, nil
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
	var ret C.int32_t
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
	var ret C.int32_t
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

	attributes, err := schema.Attributes()
	if err != nil {
		return nil, fmt.Errorf("Error getting MaxBufferElements for array: %s", err)
	}
	// Loop through each attribute
	for _, attribute := range attributes {

		// Check if attribute is variable attribute or not
		cellValNum, err := attribute.CellValNum()
		if err != nil {
			return nil, fmt.Errorf("Error getting MaxBufferElements for array: %s", err)
		}

		// Get datatype size to convert byte lengths to needed buffer sizes
		dataType, err := attribute.Type()
		dataTypeSize := dataType.Size()

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

	// Handle coordinates
	domain, err := schema.Domain()
	if err != nil {
		return nil, fmt.Errorf("Could not get domain for MaxBufferElements: %s", err)
	}
	domainType, err := domain.Type()
	if err != nil {
		return nil, fmt.Errorf("Could not get domainType for MaxBufferElements: %s", err)
	}
	bufferValSize, err := a.MaxBufferSize(TILEDB_COORDS, subarray)
	if err != nil {
		return nil, fmt.Errorf("Error getting MaxBufferElements for array: %s", err)
	}
	ret[TILEDB_COORDS] = [2]uint64{0, bufferValSize / domainType.Size()}

	return ret, nil
}

// URI returns the array's uri
func (a *Array) URI() (string, error) {
	var curi *C.char
	defer C.free(unsafe.Pointer(curi))
	C.tiledb_array_get_uri(a.context.tiledbContext, a.tiledbArray, &curi)
	uri := C.GoString(curi)
	if uri == "" {
		return uri, fmt.Errorf("Error getting URI for array: uri is empty")
	}
	return uri, nil
}

// PutCharMetadata adds char metadata to array
func (a *Array) PutCharMetadata(key string, charData string) error {
	ckey := C.CString(key)
	defer C.free(unsafe.Pointer(ckey))

	var datatype Datatype = TILEDB_CHAR

	valueNum := C.uint(len(charData))
	ret := C.tiledb_array_put_metadata(a.context.tiledbContext, a.tiledbArray,
		ckey, C.tiledb_datatype_t(datatype), valueNum, unsafe.Pointer(&[]byte(charData)[0]))

	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error adding char metadata to array: %s", a.context.LastError())
	}

	return nil
}

// PutMetadata puts a metadata key-value item to an open array. The array must
// be opened in WRITE mode, otherwise the function will error out.
func (a *Array) PutMetadata(key string, value interface{}) error {
	ckey := C.CString(key)
	defer C.free(unsafe.Pointer(ckey))

	var isSliceValue bool = false
	if reflect.TypeOf(value).Kind() == reflect.Slice {
		isSliceValue = true
	}

	var datatype Datatype
	var valueNum C.uint
	var valueType reflect.Kind

	valueInterfaceVal := reflect.ValueOf(value)
	if isSliceValue {
		if valueInterfaceVal.Len() == 0 {
			return fmt.Errorf("Value passed must be a non-empty slice, size of slice is: %d", valueInterfaceVal.Len())
		}
		valueType = reflect.TypeOf(value).Elem().Kind()
		valueNum = C.uint(valueInterfaceVal.Len())
	} else {
		valueType = reflect.TypeOf(value).Kind()
		valueNum = 1
	}

	var ret C.int32_t
	switch valueType {
	case reflect.Int:
		// Check size of int on platform
		if strconv.IntSize == 32 {
			datatype = TILEDB_INT32
			if isSliceValue {
				tmpValue := value.([]int32)
				ret = C.tiledb_array_put_metadata(a.context.tiledbContext, a.tiledbArray, ckey, C.tiledb_datatype_t(datatype), valueNum, unsafe.Pointer(&tmpValue[0]))
			} else {
				tmpValue := value.(int32)
				ret = C.tiledb_array_put_metadata(a.context.tiledbContext, a.tiledbArray, ckey, C.tiledb_datatype_t(datatype), valueNum, unsafe.Pointer(&tmpValue))
			}
		} else {
			datatype = TILEDB_INT64
			if isSliceValue {
				tmpValue := value.([]int64)
				ret = C.tiledb_array_put_metadata(a.context.tiledbContext, a.tiledbArray, ckey, C.tiledb_datatype_t(datatype), valueNum, unsafe.Pointer(&tmpValue[0]))
			} else {
				tmpValue := value.(int64)
				ret = C.tiledb_array_put_metadata(a.context.tiledbContext, a.tiledbArray, ckey, C.tiledb_datatype_t(datatype), valueNum, unsafe.Pointer(&tmpValue))
			}
		}
	case reflect.Int8:
		datatype = TILEDB_INT8
		if isSliceValue {
			tmpValue := value.([]int8)
			ret = C.tiledb_array_put_metadata(a.context.tiledbContext, a.tiledbArray, ckey, C.tiledb_datatype_t(datatype), valueNum, unsafe.Pointer(&tmpValue[0]))
		} else {
			tmpValue := value.(int8)
			ret = C.tiledb_array_put_metadata(a.context.tiledbContext, a.tiledbArray, ckey, C.tiledb_datatype_t(datatype), valueNum, unsafe.Pointer(&tmpValue))
		}
	case reflect.Int16:
		datatype = TILEDB_INT16
		if isSliceValue {
			tmpValue := value.([]int16)
			ret = C.tiledb_array_put_metadata(a.context.tiledbContext, a.tiledbArray, ckey, C.tiledb_datatype_t(datatype), valueNum, unsafe.Pointer(&tmpValue[0]))
		} else {
			tmpValue := value.(int16)
			ret = C.tiledb_array_put_metadata(a.context.tiledbContext, a.tiledbArray, ckey, C.tiledb_datatype_t(datatype), valueNum, unsafe.Pointer(&tmpValue))
		}
	case reflect.Int32:
		datatype = TILEDB_INT32
		if isSliceValue {
			tmpValue := value.([]int32)
			ret = C.tiledb_array_put_metadata(a.context.tiledbContext, a.tiledbArray, ckey, C.tiledb_datatype_t(datatype), valueNum, unsafe.Pointer(&tmpValue[0]))
		} else {
			tmpValue := value.(int32)
			ret = C.tiledb_array_put_metadata(a.context.tiledbContext, a.tiledbArray, ckey, C.tiledb_datatype_t(datatype), valueNum, unsafe.Pointer(&tmpValue))
		}
	case reflect.Int64:
		datatype = TILEDB_INT64
		if isSliceValue {
			tmpValue := value.([]int64)
			ret = C.tiledb_array_put_metadata(a.context.tiledbContext, a.tiledbArray, ckey, C.tiledb_datatype_t(datatype), valueNum, unsafe.Pointer(&tmpValue[0]))
		} else {
			tmpValue := value.(int64)
			ret = C.tiledb_array_put_metadata(a.context.tiledbContext, a.tiledbArray, ckey, C.tiledb_datatype_t(datatype), valueNum, unsafe.Pointer(&tmpValue))
		}
	case reflect.Uint:
		// Check size of uint on platform
		if strconv.IntSize == 32 {
			datatype = TILEDB_UINT32
			if isSliceValue {
				tmpValue := value.([]uint32)
				ret = C.tiledb_array_put_metadata(a.context.tiledbContext, a.tiledbArray, ckey, C.tiledb_datatype_t(datatype), valueNum, unsafe.Pointer(&tmpValue[0]))
			} else {
				tmpValue := value.(uint32)
				ret = C.tiledb_array_put_metadata(a.context.tiledbContext, a.tiledbArray, ckey, C.tiledb_datatype_t(datatype), valueNum, unsafe.Pointer(&tmpValue))
			}
		} else {
			datatype = TILEDB_UINT64
			if isSliceValue {
				tmpValue := value.([]uint64)
				ret = C.tiledb_array_put_metadata(a.context.tiledbContext, a.tiledbArray, ckey, C.tiledb_datatype_t(datatype), valueNum, unsafe.Pointer(&tmpValue[0]))
			} else {
				tmpValue := value.(uint64)
				ret = C.tiledb_array_put_metadata(a.context.tiledbContext, a.tiledbArray, ckey, C.tiledb_datatype_t(datatype), valueNum, unsafe.Pointer(&tmpValue))
			}
		}
	case reflect.Uint8:
		datatype = TILEDB_UINT8
		if isSliceValue {
			tmpValue := value.([]uint8)
			ret = C.tiledb_array_put_metadata(a.context.tiledbContext, a.tiledbArray, ckey, C.tiledb_datatype_t(datatype), valueNum, unsafe.Pointer(&tmpValue[0]))
		} else {
			tmpValue := value.(uint8)
			ret = C.tiledb_array_put_metadata(a.context.tiledbContext, a.tiledbArray, ckey, C.tiledb_datatype_t(datatype), valueNum, unsafe.Pointer(&tmpValue))
		}
	case reflect.Uint16:
		datatype = TILEDB_UINT16
		if isSliceValue {
			tmpValue := value.([]uint16)
			ret = C.tiledb_array_put_metadata(a.context.tiledbContext, a.tiledbArray, ckey, C.tiledb_datatype_t(datatype), valueNum, unsafe.Pointer(&tmpValue[0]))
		} else {
			tmpValue := value.(uint16)
			ret = C.tiledb_array_put_metadata(a.context.tiledbContext, a.tiledbArray, ckey, C.tiledb_datatype_t(datatype), valueNum, unsafe.Pointer(&tmpValue))
		}
	case reflect.Uint32:
		datatype = TILEDB_UINT32
		if isSliceValue {
			tmpValue := value.([]uint32)
			ret = C.tiledb_array_put_metadata(a.context.tiledbContext, a.tiledbArray, ckey, C.tiledb_datatype_t(datatype), valueNum, unsafe.Pointer(&tmpValue[0]))
		} else {
			tmpValue := value.(uint32)
			ret = C.tiledb_array_put_metadata(a.context.tiledbContext, a.tiledbArray, ckey, C.tiledb_datatype_t(datatype), valueNum, unsafe.Pointer(&tmpValue))
		}
	case reflect.Uint64:
		datatype = TILEDB_UINT64
		if isSliceValue {
			tmpValue := value.([]uint64)
			ret = C.tiledb_array_put_metadata(a.context.tiledbContext, a.tiledbArray, ckey, C.tiledb_datatype_t(datatype), valueNum, unsafe.Pointer(&tmpValue[0]))
		} else {
			tmpValue := value.(uint64)
			ret = C.tiledb_array_put_metadata(a.context.tiledbContext, a.tiledbArray, ckey, C.tiledb_datatype_t(datatype), valueNum, unsafe.Pointer(&tmpValue))
		}
	case reflect.Float32:
		datatype = TILEDB_FLOAT32
		if isSliceValue {
			tmpValue := value.([]float32)
			ret = C.tiledb_array_put_metadata(a.context.tiledbContext, a.tiledbArray, ckey, C.tiledb_datatype_t(datatype), valueNum, unsafe.Pointer(&tmpValue[0]))
		} else {
			tmpValue := value.(float32)
			ret = C.tiledb_array_put_metadata(a.context.tiledbContext, a.tiledbArray, ckey, C.tiledb_datatype_t(datatype), valueNum, unsafe.Pointer(&tmpValue))
		}
	case reflect.Float64:
		datatype = TILEDB_FLOAT64
		if isSliceValue {
			tmpValue := value.([]float64)
			ret = C.tiledb_array_put_metadata(a.context.tiledbContext, a.tiledbArray, ckey, C.tiledb_datatype_t(datatype), valueNum, unsafe.Pointer(&tmpValue[0]))
		} else {
			tmpValue := value.(float64)
			ret = C.tiledb_array_put_metadata(a.context.tiledbContext, a.tiledbArray, ckey, C.tiledb_datatype_t(datatype), valueNum, unsafe.Pointer(&tmpValue))
		}
	case reflect.String:
		datatype = TILEDB_STRING_UTF8
		stringValue := value.(string)
		valueNum = C.uint(len(stringValue))
		cTmpValue := C.CString(stringValue)
		defer C.free(unsafe.Pointer(cTmpValue))
		if valueNum > 0 {
			ret = C.tiledb_array_put_metadata(a.context.tiledbContext, a.tiledbArray, ckey, C.tiledb_datatype_t(datatype), valueNum, unsafe.Pointer(cTmpValue))
		}
	default:
		if isSliceValue {
			return fmt.Errorf("Unrecognized value type passed: %s", valueInterfaceVal.Index(0).Kind().String())
		}
		return fmt.Errorf("Unrecognized value type passed: %s", valueInterfaceVal.Kind().String())
	}

	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error adding metadata to array: %s", a.context.LastError())
	}
	return nil
}

// DeleteMetadata deletes a metadata key-value item from an open array. The array must
// be opened in WRITE mode, otherwise the function will error out.
func (a *Array) DeleteMetadata(key string) error {
	ckey := C.CString(key)
	defer C.free(unsafe.Pointer(ckey))

	ret := C.tiledb_array_delete_metadata(a.context.tiledbContext, a.tiledbArray, ckey)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error deleting metadata from array: %s", a.context.LastError())
	}
	return nil
}

// GetMetadata gets a metadata key-value item from an open array. The array must
// be opened in READ mode, otherwise the function will error out.
func (a *Array) GetMetadata(key string) (Datatype, uint, interface{}, error) {
	ckey := C.CString(key)
	defer C.free(unsafe.Pointer(ckey))

	var cType C.tiledb_datatype_t
	var cValueNum C.uint
	var cvalue unsafe.Pointer

	ret := C.tiledb_array_get_metadata(a.context.tiledbContext, a.tiledbArray, ckey, &cType, &cValueNum, &cvalue)
	if ret != C.TILEDB_OK {
		return 0, 0, nil, fmt.Errorf("Error getting metadata from array: %s, key: %s", a.context.LastError(), key)
	}

	valueNum := uint(cValueNum)
	if valueNum == 0 {
		return 0, 0, nil, fmt.Errorf("Error getting metadata from array, key: %s does not exist", key)
	}

	if cvalue == nil {
		return 0, 0, nil, fmt.Error("Error getting metadata from array, value is empty")
	}

	datatype := Datatype(cType)
	value, err := getMetadataValue(datatype, valueNum, cvalue)
	if err != nil {
		return 0, 0, nil, fmt.Errorf("%s, key: %s", err.Error(), key)
	}

	return datatype, valueNum, value, nil
}

// GetMetadataNum gets then number of metadata items in an open array. The array must
// be opened in READ mode, otherwise the function will error out.
func (a *Array) GetMetadataNum() (uint64, error) {
	var cNum C.uint64_t

	ret := C.tiledb_array_get_metadata_num(a.context.tiledbContext, a.tiledbArray, &cNum)
	if ret != C.TILEDB_OK {
		return 0, fmt.Errorf("Error getting number of metadata from array: %s", a.context.LastError())
	}

	return uint64(cNum), nil
}

// GetMetadataFromIndex gets a metadata item from an open array using an index.
// The array must be opened in READ mode, otherwise the function will
// error out.
func (a *Array) GetMetadataFromIndex(index uint64) (*ArrayMetadata, error) {
	return a.GetMetadataFromIndexWithValueLimit(index, nil)
}

// GetMetadataFromIndexWithValueLimit gets a metadata item from an open array using an index.
// The array must be opened in READ mode, otherwise the function will
// error out.
// limit parameter limits the number of values returned if string or array
// This is helpful for pushdown of limitting metadata. If nil value is returned
// in full
func (a *Array) GetMetadataFromIndexWithValueLimit(index uint64, limit *uint) (*ArrayMetadata, error) {
	var cKey *C.char
	defer C.free(unsafe.Pointer(cKey))
	var cIndex C.uint64_t = C.uint64_t(index)
	var cType C.tiledb_datatype_t
	var cKeyLen C.uint32_t
	var cValueNum C.uint
	var cvalue unsafe.Pointer

	ret := C.tiledb_array_get_metadata_from_index(a.context.tiledbContext,
		a.tiledbArray, cIndex, &cKey, &cKeyLen, &cType, &cValueNum, &cvalue)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error getting metadata from array: %s, Index: %d", a.context.LastError(), index)
	}

	valueNum := uint(cValueNum)
	if valueNum == 0 {
		return nil, fmt.Errorf("Error getting metadata from array, Index: %d does not exist", index)
	}

	if cvalue == nil {
		return 0, 0, nil, fmt.Error("Error getting metadata from array, value is empty")
	}

	datatype := Datatype(cType)
	if limit != nil && valueNum > *limit {
		valueNum = *limit
	}
	value, err := getMetadataValue(datatype, valueNum, cvalue)
	if err != nil {
		return nil, fmt.Errorf("%s, Index: %d", err.Error(), index)
	}

	arrayMetadata := ArrayMetadata{
		Key:      C.GoString(cKey),
		KeyLen:   uint32(cKeyLen),
		Datatype: datatype,
		ValueNum: valueNum,
		Value:    value,
	}

	return &arrayMetadata, nil
}

// GetMetadataMap returns a map[string]*ArrayMetadata where key is the key of
// each metadata added and value is an ArrayMetadata struct. The map contains
// all array metadata previously added
func (a *Array) GetMetadataMap() (map[string]*ArrayMetadata, error) {
	return a.GetMetadataMapWithValueLimit(nil)
}

// GetMetadataMapWithValueLimit returns a map[string]*ArrayMetadata where key is the key of
// each metadata added and value is an ArrayMetadata struct. The map contains
// all array metadata previously added
// limit parameter limits the number of values returned if string or array
// This is helpful for pushdown of limitting metadata. If nil value is returned
// in full
func (a *Array) GetMetadataMapWithValueLimit(limit *uint) (map[string]*ArrayMetadata, error) {
	metadataMap := make(map[string]*ArrayMetadata)

	numOfMetadata, err := a.GetMetadataNum()
	if err != nil {
		return nil, err
	}

	var I uint64
	for I = 0; I < numOfMetadata; I++ {
		arrayMetadata, err := a.GetMetadataFromIndexWithValueLimit(I, limit)
		if err != nil {
			return nil, err
		}
		metadataMap[arrayMetadata.Key] = arrayMetadata
	}

	return metadataMap, nil
}

// ConsolidateMetadata consolidates the array metadata into a single array
// metadata file.
// You must first finalize all queries to the array before consolidation can
// begin (as consolidation temporarily acquires an exclusive lock on the array).
func (a *Array) ConsolidateMetadata(config *Config) error {
	curi := C.CString(a.uri)
	defer C.free(unsafe.Pointer(curi))

	var ret C.int32_t
	if config == nil {
		ret = C.tiledb_array_consolidate_metadata(a.context.tiledbContext, curi, nil)
	} else {
		ret = C.tiledb_array_consolidate_metadata(a.context.tiledbContext, curi, config.tiledbConfig)
	}
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error consolidating array metadata")
	}

	return nil
}

// ConsolidateMetadataWithKey consolidates the array metadata of an encrypted
// array into a single file.
// You must first finalize all queries to the array before consolidation can
// begin (as consolidation temporarily acquires an exclusive lock on the array).
func (a *Array) ConsolidateMetadataWithKey(encryptionType EncryptionType, key string, config *Config) error {
	ckey := unsafe.Pointer(C.CString(key))
	defer C.free(ckey)
	curi := C.CString(a.uri)
	defer C.free(unsafe.Pointer(curi))

	var ret C.int32_t
	if config == nil {
		ret = C.tiledb_array_consolidate_metadata_with_key(a.context.tiledbContext,
			curi, C.tiledb_encryption_type_t(encryptionType), ckey, C.uint32_t(len(key)), nil)
	} else {
		ret = C.tiledb_array_consolidate_metadata_with_key(a.context.tiledbContext,
			curi, C.tiledb_encryption_type_t(encryptionType), ckey, C.uint32_t(len(key)), config.tiledbConfig)
	}
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error consolidating array metadata")
	}

	return nil
}

func getMetadataValue(datatype Datatype, valueNum uint, cvalue unsafe.Pointer) (interface{}, error) {
	var value interface{}
	switch datatype {
	case TILEDB_INT8:
		if valueNum > 1 {
			tmpValue := make([]int8, valueNum)
			tmpslice := (*[1 << 46]C.int8_t)(unsafe.Pointer(cvalue))[:valueNum:valueNum]
			for i, s := range tmpslice {
				tmpValue[i] = int8(s)
			}
			value = tmpValue
		} else {
			value = *(*int8)(unsafe.Pointer(cvalue))
		}
	case TILEDB_INT16:
		if valueNum > 1 {
			tmpValue := make([]int16, valueNum)
			tmpslice := (*[1 << 46]C.int16_t)(unsafe.Pointer(cvalue))[:valueNum:valueNum]
			for i, s := range tmpslice {
				tmpValue[i] = int16(s)
			}
			value = tmpValue
		} else {
			value = *(*int16)(unsafe.Pointer(cvalue))
		}
	case TILEDB_INT32:
		if valueNum > 1 {
			tmpValue := make([]int32, valueNum)
			tmpslice := (*[1 << 46]C.int32_t)(unsafe.Pointer(cvalue))[:valueNum:valueNum]
			for i, s := range tmpslice {
				tmpValue[i] = int32(s)
			}
			value = tmpValue
		} else {
			value = *(*int32)(unsafe.Pointer(cvalue))
		}
	case TILEDB_INT64:
		if valueNum > 1 {
			tmpValue := make([]int64, valueNum)
			tmpslice := (*[1 << 46]C.int64_t)(unsafe.Pointer(cvalue))[:valueNum:valueNum]
			for i, s := range tmpslice {
				tmpValue[i] = int64(s)
			}
			value = tmpValue
		} else {
			value = *(*int64)(unsafe.Pointer(cvalue))
		}
	case TILEDB_UINT8:
		if valueNum > 1 {
			tmpValue := make([]uint8, valueNum)
			tmpslice := (*[1 << 46]C.uint8_t)(unsafe.Pointer(cvalue))[:valueNum:valueNum]
			for i, s := range tmpslice {
				tmpValue[i] = uint8(s)
			}
			value = tmpValue
		} else {
			value = *(*uint8)(unsafe.Pointer(cvalue))
		}
	case TILEDB_UINT16:
		if valueNum > 1 {
			tmpValue := make([]uint16, valueNum)
			tmpslice := (*[1 << 46]C.uint16_t)(unsafe.Pointer(cvalue))[:valueNum:valueNum]
			for i, s := range tmpslice {
				tmpValue[i] = uint16(s)
			}
			value = tmpValue
		} else {
			value = *(*uint16)(unsafe.Pointer(cvalue))
		}
	case TILEDB_UINT32:
		if valueNum > 1 {
			tmpValue := make([]uint32, valueNum)
			tmpslice := (*[1 << 46]C.uint32_t)(unsafe.Pointer(cvalue))[:valueNum:valueNum]
			for i, s := range tmpslice {
				tmpValue[i] = uint32(s)
			}
			value = tmpValue
		} else {
			value = *(*uint32)(unsafe.Pointer(cvalue))
		}
	case TILEDB_UINT64:
		if valueNum > 1 {
			tmpValue := make([]uint64, valueNum)
			tmpslice := (*[1 << 46]C.uint64_t)(unsafe.Pointer(cvalue))[:valueNum:valueNum]
			for i, s := range tmpslice {
				tmpValue[i] = uint64(s)
			}
			value = tmpValue
		} else {
			value = *(*uint64)(unsafe.Pointer(cvalue))
		}
	case TILEDB_FLOAT32:
		if valueNum > 1 {
			tmpValue := make([]float32, valueNum)
			tmpslice := (*[1 << 46]C.float)(unsafe.Pointer(cvalue))[:valueNum:valueNum]
			for i, s := range tmpslice {
				tmpValue[i] = float32(s)
			}
			value = tmpValue
		} else {
			value = *(*float32)(unsafe.Pointer(cvalue))
		}
	case TILEDB_FLOAT64:
		if valueNum > 1 {
			tmpValue := make([]float64, valueNum)
			tmpslice := (*[1 << 46]C.double)(unsafe.Pointer(cvalue))[:valueNum:valueNum]
			for i, s := range tmpslice {
				tmpValue[i] = float64(s)
			}
			value = tmpValue
		} else {
			value = *(*float64)(unsafe.Pointer(cvalue))
		}
	case TILEDB_CHAR:
		tmpslice := (*[1 << 46]C.char)(unsafe.Pointer(cvalue))[:valueNum:valueNum]
		value = C.GoString(&tmpslice[0])[0:valueNum]
	case TILEDB_STRING_ASCII:
		tmpslice := (*[1 << 46]C.char)(unsafe.Pointer(cvalue))[:valueNum:valueNum]
		value = C.GoString(&tmpslice[0])[0:valueNum]
	case TILEDB_STRING_UTF8:
		tmpslice := (*[1 << 46]C.char)(unsafe.Pointer(cvalue))[:valueNum:valueNum]
		value = C.GoString(&tmpslice[0])[0:valueNum]
	case TILEDB_DATETIME_YEAR, TILEDB_DATETIME_MONTH, TILEDB_DATETIME_WEEK,
		TILEDB_DATETIME_DAY, TILEDB_DATETIME_HR, TILEDB_DATETIME_MIN,
		TILEDB_DATETIME_SEC, TILEDB_DATETIME_MS, TILEDB_DATETIME_US,
		TILEDB_DATETIME_NS, TILEDB_DATETIME_PS, TILEDB_DATETIME_FS,
		TILEDB_DATETIME_AS:
		if valueNum > 1 {
			return nil, fmt.Errorf("Unrecognized value type: %d", datatype)
		} else {
			var timestamp interface{} = *(*int16)(unsafe.Pointer(cvalue))
			value = GetTimeFromTimestamp(datatype, timestamp.(int64))
		}
	default:
		return nil, fmt.Errorf("Unrecognized value type: %d", datatype)
	}

	return value, nil
}
