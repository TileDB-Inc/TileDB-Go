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
	"sync"
	"unsafe"

	"github.com/TileDB-Inc/TileDB-Go/bytesizes"
)

// Query construct and execute read/write queries on a tiledb Array
type Query struct {
	tiledbQuery          *C.tiledb_query_t
	array                *Array
	context              *Context
	config               *Config
	buffers              []interface{}
	bufferMutex          sync.Mutex
	resultBufferElements map[string][3]*uint64
}

// RangeLimits defines a query range
type RangeLimits struct {
	start interface{}
	end   interface{}
}

// MarshalJSON implements the Marshaler interface for RangeLimits
func (r RangeLimits) MarshalJSON() ([]byte, error) {
	rangeLimitMap := make(map[string]interface{})
	rangeLimitMap["end"] = r.end
	rangeLimitMap["start"] = r.start

	return json.Marshal(rangeLimitMap)
}

/*
NewQuery Creates a TileDB query object.

The query type (read or write) must be the same as the type used
to open the array object.

The storage manager also acquires a shared lock on the array.
This means multiple read and write queries to the same array can be made
concurrently (in TileDB, only consolidation requires an exclusive lock for
a short period of time).
*/
func NewQuery(tdbCtx *Context, array *Array) (*Query, error) {
	if array == nil {
		return nil, fmt.Errorf("Error creating tiledb query: passed array is nil")
	}

	queryType, err := array.QueryType()
	if err != nil {
		return nil, fmt.Errorf("Error getting QueryType from passed array %s", err)
	}

	query := Query{context: tdbCtx, array: array}
	ret := C.tiledb_query_alloc(query.context.tiledbContext, array.tiledbArray, C.tiledb_query_type_t(queryType), &query.tiledbQuery)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error creating tiledb query: %s", query.context.LastError())
	}

	// Set finalizer for free C pointer on gc
	runtime.SetFinalizer(&query, func(query *Query) {
		query.Free()
	})

	query.resultBufferElements = make(map[string][3]*uint64)

	return &query, nil
}

// Free tiledb_query_t that was allocated on heap in c
func (q *Query) Free() {
	q.bufferMutex.Lock()
	defer q.bufferMutex.Unlock()
	q.buffers = nil
	q.resultBufferElements = nil
	if q.tiledbQuery != nil {
		C.tiledb_query_free(&q.tiledbQuery)
	}
}

// SetSubArray Sets a subarray, defined in the order dimensions were added.
// Coordinates are inclusive. For the case of writes, this is meaningful only
// for dense arrays, and specifically dense writes.
func (q *Query) SetSubArray(subArray interface{}) error {

	if reflect.TypeOf(subArray).Kind() != reflect.Slice {
		return fmt.Errorf("Subarray passed must be a slice, type passed was: %s", reflect.TypeOf(subArray).Kind().String())
	}

	subArrayType := reflect.TypeOf(subArray).Elem().Kind()

	schema, err := q.array.Schema()
	if err != nil {
		return fmt.Errorf("Could not get array schema from query array: %s", err)
	}

	domain, err := schema.Domain()
	if err != nil {
		return fmt.Errorf("Could not get domain from array schema: %s", err)
	}

	domainType, err := domain.Type()
	if err != nil {
		return fmt.Errorf("Could not get domain type: %s", err)
	}

	if subArrayType != domainType.ReflectKind() {
		return fmt.Errorf("Domain and subarray do not have the same data types. Domain: %s, Extent: %s", domainType.ReflectKind().String(), subArrayType.String())
	}

	var csubArray unsafe.Pointer
	switch subArrayType {
	case reflect.Int:
		// Create subArray void*
		tmpSubArray := subArray.([]int)
		csubArray = unsafe.Pointer(&tmpSubArray[0])
	case reflect.Int8:
		// Create subArray void*
		tmpSubArray := subArray.([]int8)
		csubArray = unsafe.Pointer(&tmpSubArray[0])
	case reflect.Int16:
		// Create subArray void*
		tmpSubArray := subArray.([]int16)
		csubArray = unsafe.Pointer(&tmpSubArray[0])
	case reflect.Int32:
		// Create subArray void*
		tmpSubArray := subArray.([]int32)
		csubArray = unsafe.Pointer(&tmpSubArray[0])
	case reflect.Int64:
		// Create subArray void*
		tmpSubArray := subArray.([]int64)
		csubArray = unsafe.Pointer(&tmpSubArray[0])
	case reflect.Uint:
		// Create subArray void*
		tmpSubArray := subArray.([]uint)
		csubArray = unsafe.Pointer(&tmpSubArray[0])
	case reflect.Uint8:
		// Create subArray void*
		tmpSubArray := subArray.([]uint8)
		csubArray = unsafe.Pointer(&tmpSubArray[0])
	case reflect.Uint16:
		// Create subArray void*
		tmpSubArray := subArray.([]uint16)
		csubArray = unsafe.Pointer(&tmpSubArray[0])
	case reflect.Uint32:
		// Create subArray void*
		tmpSubArray := subArray.([]uint32)
		csubArray = unsafe.Pointer(&tmpSubArray[0])
	case reflect.Uint64:
		// Create subArray void*
		tmpSubArray := subArray.([]uint64)
		csubArray = unsafe.Pointer(&tmpSubArray[0])
	case reflect.Float32:
		// Create subArray void*
		tmpSubArray := subArray.([]float32)
		csubArray = unsafe.Pointer(&tmpSubArray[0])
	case reflect.Float64:
		// Create subArray void*
		tmpSubArray := subArray.([]float64)
		csubArray = unsafe.Pointer(&tmpSubArray[0])
	default:
		return fmt.Errorf("Unrecognized subArray type passed: %s", subArrayType.String())
	}

	ret := C.tiledb_query_set_subarray(q.context.tiledbContext, q.tiledbQuery, csubArray)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error setting query subarray: %s", q.context.LastError())
	}
	return nil
}

// SetBufferUnsafe Sets the buffer for a fixed-sized attribute to a query
// This takes an unsafe pointer which is passsed straight to tiledb c_api
// for advanced usage
func (q *Query) SetBufferUnsafe(attribute string, buffer unsafe.Pointer, bufferSize uint64) (*uint64, error) {
	cAttribute := C.CString(attribute)
	defer C.free(unsafe.Pointer(cAttribute))

	ret := C.tiledb_query_set_buffer(
		q.context.tiledbContext,
		q.tiledbQuery,
		cAttribute,
		buffer,
		(*C.uint64_t)(unsafe.Pointer(&bufferSize)))

	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf(
			"Error setting query buffer: %s", q.context.LastError())
	}

	q.resultBufferElements[attribute] = [3]*uint64{nil, &bufferSize, nil}

	return &bufferSize, nil
}

// SetBuffer Sets the buffer for a fixed-sized attribute to a query
// The buffer must be an initialized slice
func (q *Query) SetBuffer(attributeOrDimension string, buffer interface{}) (*uint64,
	error) {
	bufferReflectType := reflect.TypeOf(buffer)
	bufferReflectValue := reflect.ValueOf(buffer)
	if bufferReflectValue.Kind() != reflect.Slice {
		return nil, fmt.Errorf(
			"Buffer passed must be a slice that is pre"+
				"-allocated, type passed was: %s",
			bufferReflectValue.Kind().String())
	}

	// Next get the attribute to validate the buffer type is the same as the attribute
	schema, err := q.array.Schema()
	if err != nil {
		return nil, fmt.Errorf(
			"Could not get array schema for SetBuffer: %s",
			err)
	}

	domain, err := schema.Domain()
	if err != nil {
		return nil, fmt.Errorf(
			"Could not get domain for SetBuffer: %s",
			attributeOrDimension)
	}

	var attributeOrDimensionType Datatype
	// If we are setting tiledb coordinates for a sparse array we want to check
	// the domain type. The TILEDB_COORDS attribute is only materialized after
	// the first write
	if attributeOrDimension == TILEDB_COORDS {
		attributeOrDimensionType, err = domain.Type()
		if err != nil {
			return nil, fmt.Errorf(
				"Could not get domainType for SetBuffer: %s",
				attributeOrDimension)
		}
	} else {
		hasDim, err := domain.HasDimension(attributeOrDimension)
		if err != nil {
			return nil, err
		}

		if hasDim {
			dimension, err := domain.DimensionFromName(attributeOrDimension)
			if err != nil {
				return nil, fmt.Errorf("Could not get attribute or dimension for SetBuffer: %s",
					attributeOrDimension)
			}

			attributeOrDimensionType, err = dimension.Type()
			if err != nil {
				return nil, fmt.Errorf("Could not get dimensionType for SetBuffer: %s",
					attributeOrDimension)
			}
		} else {
			schemaAttribute, err := schema.AttributeFromName(attributeOrDimension)
			if err != nil {
				return nil, fmt.Errorf("Could not get attribute %s for SetBuffer",
					attributeOrDimension)
			}

			attributeOrDimensionType, err = schemaAttribute.Type()
			if err != nil {
				return nil, fmt.Errorf("Could not get attributeType for SetBuffer: %s",
					attributeOrDimension)
			}
		}
	}

	bufferType := bufferReflectType.Elem().Kind()
	if attributeOrDimensionType.ReflectKind() != bufferType {
		return nil, fmt.Errorf("Buffer and Attribute do not have the same"+
			" data types. Buffer: %s, Attribute: %s",
			bufferType.String(),
			attributeOrDimensionType.ReflectKind().String())
	}

	var cbuffer unsafe.Pointer
	// Get length of slice, this will be multiplied by size of datatype below
	bufferSize := uint64(bufferReflectValue.Len())

	if bufferSize == uint64(0) {
		return nil, fmt.Errorf(
			"Buffer has no length, vbuffers are required to be " +
				"initialized before reading or writting")
	}

	// Acquire a lock to make appending to buffer slice thread safe
	q.bufferMutex.Lock()
	defer q.bufferMutex.Unlock()

	switch bufferType {
	case reflect.Int:
		// Set buffersize
		bufferSize = bufferSize * bytesizes.Int
		// Create buffer void*
		tmpBuffer := buffer.([]int)
		// Store slice so underlying array is not gc'ed
		q.buffers = append(q.buffers, tmpBuffer)
		cbuffer = unsafe.Pointer(&(tmpBuffer)[0])
	case reflect.Int8:
		// Set buffersize
		bufferSize = bufferSize * bytesizes.Int8
		// Create buffer void*
		tmpBuffer := buffer.([]int8)
		// Store slice so underlying array is not gc'ed
		q.buffers = append(q.buffers, tmpBuffer)
		cbuffer = unsafe.Pointer(&(tmpBuffer)[0])
	case reflect.Int16:
		// Set buffersize
		bufferSize = bufferSize * bytesizes.Int16
		// Create buffer void*
		tmpBuffer := buffer.([]int16)
		// Store slice so underlying array is not gc'ed
		q.buffers = append(q.buffers, tmpBuffer)
		cbuffer = unsafe.Pointer(&(tmpBuffer)[0])
	case reflect.Int32:
		// Set buffersize
		bufferSize = bufferSize * bytesizes.Int32
		// Create buffer void*
		tmpBuffer := buffer.([]int32)
		// Store slice so underlying array is not gc'ed
		q.buffers = append(q.buffers, tmpBuffer)
		cbuffer = unsafe.Pointer(&(tmpBuffer)[0])
	case reflect.Int64:
		// Set buffersize
		bufferSize = bufferSize * bytesizes.Int64
		// Create buffer void*
		tmpBuffer := buffer.([]int64)
		// Store slice so underlying array is not gc'ed
		q.buffers = append(q.buffers, tmpBuffer)
		cbuffer = unsafe.Pointer(&(tmpBuffer)[0])
	case reflect.Uint:
		// Set buffersize
		bufferSize = bufferSize * bytesizes.Uint
		// Create buffer void*
		tmpBuffer := buffer.([]uint)
		// Store slice so underlying array is not gc'ed
		q.buffers = append(q.buffers, tmpBuffer)
		cbuffer = unsafe.Pointer(&(tmpBuffer)[0])
	case reflect.Uint8:
		// Set buffersize
		bufferSize = bufferSize * bytesizes.Uint8
		// Create buffer void*
		tmpBuffer := buffer.([]uint8)
		// Store slice so underlying array is not gc'ed
		q.buffers = append(q.buffers, tmpBuffer)
		cbuffer = unsafe.Pointer(&(tmpBuffer)[0])
	case reflect.Uint16:
		// Set buffersize
		bufferSize = bufferSize * bytesizes.Uint16
		// Create buffer void*
		tmpBuffer := buffer.([]uint16)
		// Store slice so underlying array is not gc'ed
		q.buffers = append(q.buffers, tmpBuffer)
		cbuffer = unsafe.Pointer(&(tmpBuffer)[0])
	case reflect.Uint32:
		// Set buffersize
		bufferSize = bufferSize * bytesizes.Uint32
		// Create buffer void*
		tmpBuffer := buffer.([]uint32)
		// Store slice so underlying array is not gc'ed
		q.buffers = append(q.buffers, tmpBuffer)
		cbuffer = unsafe.Pointer(&(tmpBuffer)[0])
	case reflect.Uint64:
		// Set buffersize
		bufferSize = bufferSize * bytesizes.Uint64
		// Create buffer void*
		tmpBuffer := buffer.([]uint64)
		// Store slice so underlying array is not gc'ed
		q.buffers = append(q.buffers, tmpBuffer)
		cbuffer = unsafe.Pointer(&(tmpBuffer)[0])
	case reflect.Float32:
		// Set buffersize
		bufferSize = bufferSize * bytesizes.Float32
		// Create buffer void*
		tmpBuffer := buffer.([]float32)
		// Store slice so underlying array is not gc'ed
		q.buffers = append(q.buffers, tmpBuffer)
		cbuffer = unsafe.Pointer(&(tmpBuffer)[0])
	case reflect.Float64:
		// Set buffersize
		bufferSize = bufferSize * bytesizes.Float64
		// Create buffer void*
		tmpBuffer := buffer.([]float64)
		// Store slice so underlying array is not gc'ed
		q.buffers = append(q.buffers, tmpBuffer)
		cbuffer = unsafe.Pointer(&(tmpBuffer)[0])
	default:
		return nil,
			fmt.Errorf("Unrecognized buffer type passed: %s",
				bufferType.String())
	}

	cAttributeOrDimension := C.CString(attributeOrDimension)
	defer C.free(unsafe.Pointer(cAttributeOrDimension))

	ret := C.tiledb_query_set_buffer(
		q.context.tiledbContext,
		q.tiledbQuery,
		cAttributeOrDimension,
		cbuffer,
		(*C.uint64_t)(unsafe.Pointer(&bufferSize)))

	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf(
			"Error setting query buffer: %s", q.context.LastError())
	}

	q.resultBufferElements[attributeOrDimension] =
		[3]*uint64{nil, &bufferSize, nil}

	return &bufferSize, nil
}

// SetBufferNullableUnsafe Sets the buffer for a fixed-sized nullable attribute to a query
// This takes an unsafe pointer which is passsed straight to tiledb c_api
// for advanced usage
func (q *Query) SetBufferNullableUnsafe(attribute string, buffer unsafe.Pointer, bufferSize uint64, bufferValidity unsafe.Pointer, bufferValiditySize uint64) (*uint64, *uint64, error) {
	cAttribute := C.CString(attribute)
	defer C.free(unsafe.Pointer(cAttribute))

	ret := C.tiledb_query_set_buffer_nullable(
		q.context.tiledbContext,
		q.tiledbQuery,
		cAttribute,
		buffer,
		(*C.uint64_t)(unsafe.Pointer(&bufferSize)),
		(*C.uint8_t)(bufferValidity),
		(*C.uint64_t)(unsafe.Pointer(&bufferValiditySize)))

	if ret != C.TILEDB_OK {
		return nil, nil, fmt.Errorf(
			"Error setting query nullable buffer: %s", q.context.LastError())
	}

	q.resultBufferElements[attribute] = [3]*uint64{nil, &bufferSize, &bufferValiditySize}

	return &bufferSize, &bufferValiditySize, nil
}

// SetBufferNullable Sets the buffer for a fixed-sized nullable attribute to a query
// The buffer must be an initialized slice
func (q *Query) SetBufferNullable(attributeOrDimension string, buffer interface{}, bufferValidity []uint8) (*uint64, *uint64, error) {
	bufferReflectType := reflect.TypeOf(buffer)
	bufferReflectValue := reflect.ValueOf(buffer)
	if bufferReflectValue.Kind() != reflect.Slice {
		return nil, nil, fmt.Errorf(
			"Buffer passed must be a slice that is pre"+
				"-allocated, type passed was: %s",
			bufferReflectValue.Kind().String())
	}

	// Next get the attribute to validate the buffer type is the same as the attribute
	schema, err := q.array.Schema()
	if err != nil {
		return nil, nil, fmt.Errorf(
			"Could not get array schema for SetBufferNullable: %s",
			err)
	}

	domain, err := schema.Domain()
	if err != nil {
		return nil, nil, fmt.Errorf(
			"Could not get domain for SetBufferNullable: %s",
			attributeOrDimension)
	}

	var attributeOrDimensionType Datatype
	hasDim, err := domain.HasDimension(attributeOrDimension)
	if err != nil {
		return nil, nil, err
	}

	if hasDim {
		dimension, err := domain.DimensionFromName(attributeOrDimension)
		if err != nil {
			return nil, nil, fmt.Errorf("Could not get attribute or dimension for SetBufferNullable: %s",
				attributeOrDimension)
		}

		attributeOrDimensionType, err = dimension.Type()
		if err != nil {
			return nil, nil, fmt.Errorf("Could not get dimensionType for SetBufferNullable: %s",
				attributeOrDimension)
		}
	} else {
		schemaAttribute, err := schema.AttributeFromName(attributeOrDimension)
		if err != nil {
			return nil, nil, fmt.Errorf("Could not get attribute %s for SetBufferNullable",
				attributeOrDimension)
		}

		attributeOrDimensionType, err = schemaAttribute.Type()
		if err != nil {
			return nil, nil, fmt.Errorf("Could not get attributeType for SetBufferNullable: %s",
				attributeOrDimension)
		}
	}

	bufferType := bufferReflectType.Elem().Kind()
	if attributeOrDimensionType.ReflectKind() != bufferType {
		return nil, nil, fmt.Errorf("Buffer and Attribute do not have the same"+
			" data types. Buffer: %s, Attribute: %s",
			bufferType.String(),
			attributeOrDimensionType.ReflectKind().String())
	}

	var cbuffer unsafe.Pointer
	// Get length of slice, this will be multiplied by size of datatype below
	bufferSize := uint64(bufferReflectValue.Len())

	if bufferSize == uint64(0) {
		return nil, nil, fmt.Errorf(
			"Buffer has no length, vbuffers are required to be " +
				"initialized before reading or writting")
	}

	// Acquire a lock to make appending to buffer slice thread safe
	q.bufferMutex.Lock()
	defer q.bufferMutex.Unlock()

	switch bufferType {
	case reflect.Int:
		// Set buffersize
		bufferSize = bufferSize * bytesizes.Int
		// Create buffer void*
		tmpBuffer := buffer.([]int)
		// Store slice so underlying array is not gc'ed
		q.buffers = append(q.buffers, tmpBuffer)
		cbuffer = unsafe.Pointer(&(tmpBuffer)[0])
	case reflect.Int8:
		// Set buffersize
		bufferSize = bufferSize * bytesizes.Int8
		// Create buffer void*
		tmpBuffer := buffer.([]int8)
		// Store slice so underlying array is not gc'ed
		q.buffers = append(q.buffers, tmpBuffer)
		cbuffer = unsafe.Pointer(&(tmpBuffer)[0])
	case reflect.Int16:
		// Set buffersize
		bufferSize = bufferSize * bytesizes.Int16
		// Create buffer void*
		tmpBuffer := buffer.([]int16)
		// Store slice so underlying array is not gc'ed
		q.buffers = append(q.buffers, tmpBuffer)
		cbuffer = unsafe.Pointer(&(tmpBuffer)[0])
	case reflect.Int32:
		// Set buffersize
		bufferSize = bufferSize * bytesizes.Int32
		// Create buffer void*
		tmpBuffer := buffer.([]int32)
		// Store slice so underlying array is not gc'ed
		q.buffers = append(q.buffers, tmpBuffer)
		cbuffer = unsafe.Pointer(&(tmpBuffer)[0])
	case reflect.Int64:
		// Set buffersize
		bufferSize = bufferSize * bytesizes.Int64
		// Create buffer void*
		tmpBuffer := buffer.([]int64)
		// Store slice so underlying array is not gc'ed
		q.buffers = append(q.buffers, tmpBuffer)
		cbuffer = unsafe.Pointer(&(tmpBuffer)[0])
	case reflect.Uint:
		// Set buffersize
		bufferSize = bufferSize * bytesizes.Uint
		// Create buffer void*
		tmpBuffer := buffer.([]uint)
		// Store slice so underlying array is not gc'ed
		q.buffers = append(q.buffers, tmpBuffer)
		cbuffer = unsafe.Pointer(&(tmpBuffer)[0])
	case reflect.Uint8:
		// Set buffersize
		bufferSize = bufferSize * bytesizes.Uint8
		// Create buffer void*
		tmpBuffer := buffer.([]uint8)
		// Store slice so underlying array is not gc'ed
		q.buffers = append(q.buffers, tmpBuffer)
		cbuffer = unsafe.Pointer(&(tmpBuffer)[0])
	case reflect.Uint16:
		// Set buffersize
		bufferSize = bufferSize * bytesizes.Uint16
		// Create buffer void*
		tmpBuffer := buffer.([]uint16)
		// Store slice so underlying array is not gc'ed
		q.buffers = append(q.buffers, tmpBuffer)
		cbuffer = unsafe.Pointer(&(tmpBuffer)[0])
	case reflect.Uint32:
		// Set buffersize
		bufferSize = bufferSize * bytesizes.Uint32
		// Create buffer void*
		tmpBuffer := buffer.([]uint32)
		// Store slice so underlying array is not gc'ed
		q.buffers = append(q.buffers, tmpBuffer)
		cbuffer = unsafe.Pointer(&(tmpBuffer)[0])
	case reflect.Uint64:
		// Set buffersize
		bufferSize = bufferSize * bytesizes.Uint64
		// Create buffer void*
		tmpBuffer := buffer.([]uint64)
		// Store slice so underlying array is not gc'ed
		q.buffers = append(q.buffers, tmpBuffer)
		cbuffer = unsafe.Pointer(&(tmpBuffer)[0])
	case reflect.Float32:
		// Set buffersize
		bufferSize = bufferSize * bytesizes.Float32
		// Create buffer void*
		tmpBuffer := buffer.([]float32)
		// Store slice so underlying array is not gc'ed
		q.buffers = append(q.buffers, tmpBuffer)
		cbuffer = unsafe.Pointer(&(tmpBuffer)[0])
	case reflect.Float64:
		// Set buffersize
		bufferSize = bufferSize * bytesizes.Float64
		// Create buffer void*
		tmpBuffer := buffer.([]float64)
		// Store slice so underlying array is not gc'ed
		q.buffers = append(q.buffers, tmpBuffer)
		cbuffer = unsafe.Pointer(&(tmpBuffer)[0])
	default:
		return nil, nil,
			fmt.Errorf("Unrecognized buffer type passed: %s",
				bufferType.String())
	}

	cAttributeOrDimension := C.CString(attributeOrDimension)
	defer C.free(unsafe.Pointer(cAttributeOrDimension))

	bufferValiditySize := uint64(len(bufferValidity)) * bytesizes.Uint8
	if bufferValiditySize == uint64(0) {
		return nil, nil, fmt.Errorf("Validity slice has no length, " +
			"offset slices are required to be initialized before reading or writting")
	}
	ret := C.tiledb_query_set_buffer_nullable(
		q.context.tiledbContext,
		q.tiledbQuery,
		cAttributeOrDimension,
		cbuffer,
		(*C.uint64_t)(unsafe.Pointer(&bufferSize)),
		(*C.uint8_t)(unsafe.Pointer(&(bufferValidity)[0])),
		(*C.uint64_t)(unsafe.Pointer(&bufferValiditySize)),
	)

	if ret != C.TILEDB_OK {
		return nil, nil, fmt.Errorf(
			"Error setting query buffer: %s", q.context.LastError())
	}

	q.resultBufferElements[attributeOrDimension] =
		[3]*uint64{nil, &bufferSize, &bufferValiditySize}

	return &bufferSize, &bufferValiditySize, nil
}

func getStartAndEndBuffers(start interface{}, end interface{}) (
	unsafe.Pointer, unsafe.Pointer, error) {
	startReflectValue := reflect.ValueOf(start)
	endReflectValue := reflect.ValueOf(end)

	if startReflectValue.Kind() != endReflectValue.Kind() {
		return nil, nil, fmt.Errorf(
			"The datatype of the range components must be the same as the type, start was: %s, end was: %s",
			startReflectValue.Kind().String(), endReflectValue.Kind().String())
	}

	var startBuffer unsafe.Pointer
	var endBuffer unsafe.Pointer

	startReflectType := reflect.TypeOf(start)
	startType := startReflectType.Kind()

	switch startType {
	case reflect.Int:
		tStart := start.(int)
		tEnd := end.(int)
		startBuffer = unsafe.Pointer(&tStart)
		endBuffer = unsafe.Pointer(&tEnd)
	case reflect.Int8:
		tStart := start.(int8)
		tEnd := end.(int8)
		startBuffer = unsafe.Pointer(&tStart)
		endBuffer = unsafe.Pointer(&tEnd)
	case reflect.Int16:
		tStart := start.(int16)
		tEnd := end.(int16)
		startBuffer = unsafe.Pointer(&tStart)
		endBuffer = unsafe.Pointer(&tEnd)
	case reflect.Int32:
		tStart := start.(int32)
		tEnd := end.(int32)
		startBuffer = unsafe.Pointer(&tStart)
		endBuffer = unsafe.Pointer(&tEnd)
	case reflect.Int64:
		tStart := start.(int64)
		tEnd := end.(int64)
		startBuffer = unsafe.Pointer(&tStart)
		endBuffer = unsafe.Pointer(&tEnd)
	case reflect.Uint:
		tStart := start.(uint)
		tEnd := end.(uint)
		startBuffer = unsafe.Pointer(&tStart)
		endBuffer = unsafe.Pointer(&tEnd)
	case reflect.Uint8:
		tStart := start.(uint8)
		tEnd := end.(uint8)
		startBuffer = unsafe.Pointer(&tStart)
		endBuffer = unsafe.Pointer(&tEnd)
	case reflect.Uint16:
		tStart := start.(uint16)
		tEnd := end.(uint16)
		startBuffer = unsafe.Pointer(&tStart)
		endBuffer = unsafe.Pointer(&tEnd)
	case reflect.Uint32:
		tStart := start.(uint32)
		tEnd := end.(uint32)
		startBuffer = unsafe.Pointer(&tStart)
		endBuffer = unsafe.Pointer(&tEnd)
	case reflect.Uint64:
		tStart := start.(uint64)
		tEnd := end.(uint64)
		startBuffer = unsafe.Pointer(&tStart)
		endBuffer = unsafe.Pointer(&tEnd)
	case reflect.Float32:
		tStart := start.(float32)
		tEnd := end.(float32)
		startBuffer = unsafe.Pointer(&tStart)
		endBuffer = unsafe.Pointer(&tEnd)
	case reflect.Float64:
		tStart := start.(float64)
		tEnd := end.(float64)
		startBuffer = unsafe.Pointer(&tStart)
		endBuffer = unsafe.Pointer(&tEnd)
	default:
		return nil, nil, fmt.Errorf("Unrecognized type of range component passed: %s",
			startType.String())
	}

	return startBuffer, endBuffer, nil
}

// AddRange adds a 1D range along a subarray dimension, which is in the form
// (start, end, stride). The datatype of the range components must be the same
// as the type of the domain of the array in the query.
// The stride is currently unsupported and set to nil.
func (q *Query) AddRange(dimIdx uint32, start interface{}, end interface{}) error {
	startBuffer, endBuffer, err := getStartAndEndBuffers(start, end)
	if err != nil {
		return err
	}

	ret := C.tiledb_query_add_range(
		q.context.tiledbContext, q.tiledbQuery,
		(C.uint32_t)(dimIdx), startBuffer, endBuffer, nil)

	if ret != C.TILEDB_OK {
		return fmt.Errorf(
			"Error adding query range: %s", q.context.LastError())
	}

	return nil
}

// AddRangeByName adds a 1D range along a subarray dimension, which is in the form
// (start, end, stride). The datatype of the range components must be the same
// as the type of the domain of the array in the query.
// The stride is currently unsupported and set to nil.
func (q *Query) AddRangeByName(dimName string, start interface{}, end interface{}) error {
	startBuffer, endBuffer, err := getStartAndEndBuffers(start, end)
	if err != nil {
		return err
	}

	cDimName := C.CString(dimName)
	defer C.free(unsafe.Pointer(cDimName))

	ret := C.tiledb_query_add_range_by_name(q.context.tiledbContext,
		q.tiledbQuery, cDimName, startBuffer, endBuffer, nil)

	if ret != C.TILEDB_OK {
		return fmt.Errorf(
			"Error adding query range: %s", q.context.LastError())
	}

	return nil
}

// AddRangeVar adds a range applicable to variable-sized dimensions
// Applicable only to string dimensions
func (q *Query) AddRangeVar(dimIdx uint32, start interface{}, end interface{}) error {
	startReflectValue := reflect.ValueOf(start)
	endReflectValue := reflect.ValueOf(end)

	if startReflectValue.Kind() != reflect.Slice {
		return fmt.Errorf("Start buffer passed must be a slice that is pre"+
			"-allocated, type passed was: %s", startReflectValue.Kind().String())
	}

	if endReflectValue.Kind() != reflect.Slice {
		return fmt.Errorf("End buffer passed must be a slice that is pre"+
			"-allocated, type passed was: %s", endReflectValue.Kind().String())
	}

	startSize := uint64(startReflectValue.Len())
	endSize := uint64(endReflectValue.Len())

	var startBuffer unsafe.Pointer
	var endBuffer unsafe.Pointer

	startReflectType := reflect.TypeOf(start)
	startType := startReflectType.Elem().Kind()

	switch startType {
	case reflect.Int:
		return fmt.Errorf("Unsupported type of range component passed: %s",
			startType.String())
	case reflect.Int8:
		return fmt.Errorf("Unsupported type of range component passed: %s",
			startType.String())
	case reflect.Int16:
		return fmt.Errorf("Unsupported type of range component passed: %s",
			startType.String())
	case reflect.Int32:
		return fmt.Errorf("Unsupported type of range component passed: %s",
			startType.String())
	case reflect.Int64:
		return fmt.Errorf("Unsupported type of range component passed: %s",
			startType.String())
	case reflect.Uint:
		return fmt.Errorf("Unsupported type of range component passed: %s",
			startType.String())
	case reflect.Uint8:
		tStart := start.([]uint8)
		tEnd := end.([]uint8)
		startBuffer = unsafe.Pointer(&(tStart[0]))
		endBuffer = unsafe.Pointer(&(tEnd[0]))

		ret := C.tiledb_query_add_range_var(
			q.context.tiledbContext, q.tiledbQuery,
			(C.uint32_t)(dimIdx), startBuffer, (C.uint64_t)(startSize), endBuffer, (C.uint64_t)(endSize))

		if ret != C.TILEDB_OK {
			return fmt.Errorf(
				"Error adding query range var: %s", q.context.LastError())
		}
	case reflect.Uint16:
		return fmt.Errorf("Unsupported type of range component passed: %s",
			startType.String())
	case reflect.Uint32:
		return fmt.Errorf("Unsupported type of range component passed: %s",
			startType.String())
	case reflect.Uint64:
		return fmt.Errorf("Unsupported type of range component passed: %s",
			startType.String())
	case reflect.Float32:
		return fmt.Errorf("Unsupported type of range component passed: %s",
			startType.String())
	case reflect.Float64:
		return fmt.Errorf("Unsupported type of range component passed: %s",
			startType.String())
	default:
		return fmt.Errorf("Unrecognized type of range component passed: %s",
			startType.String())
	}

	return nil
}

// AddRangeVarByName adds a range applicable to variable-sized dimensions
// Applicable only to string dimensions
func (q *Query) AddRangeVarByName(dimName string, start interface{}, end interface{}) error {
	startReflectValue := reflect.ValueOf(start)
	endReflectValue := reflect.ValueOf(end)

	if startReflectValue.Kind() != reflect.Slice {
		return fmt.Errorf("Start buffer passed must be a slice that is pre"+
			"-allocated, type passed was: %s", startReflectValue.Kind().String())
	}

	if endReflectValue.Kind() != reflect.Slice {
		return fmt.Errorf("End buffer passed must be a slice that is pre"+
			"-allocated, type passed was: %s", endReflectValue.Kind().String())
	}

	startSize := uint64(startReflectValue.Len())
	endSize := uint64(endReflectValue.Len())

	var startBuffer unsafe.Pointer
	var endBuffer unsafe.Pointer

	startReflectType := reflect.TypeOf(start)
	startType := startReflectType.Elem().Kind()

	switch startType {
	case reflect.Int:
		return fmt.Errorf("Unsupported type of range component passed: %s",
			startType.String())
	case reflect.Int8:
		return fmt.Errorf("Unsupported type of range component passed: %s",
			startType.String())
	case reflect.Int16:
		return fmt.Errorf("Unsupported type of range component passed: %s",
			startType.String())
	case reflect.Int32:
		return fmt.Errorf("Unsupported type of range component passed: %s",
			startType.String())
	case reflect.Int64:
		return fmt.Errorf("Unsupported type of range component passed: %s",
			startType.String())
	case reflect.Uint:
		return fmt.Errorf("Unsupported type of range component passed: %s",
			startType.String())
	case reflect.Uint8:
		tStart := start.([]uint8)
		tEnd := end.([]uint8)
		startBuffer = unsafe.Pointer(&(tStart[0]))
		endBuffer = unsafe.Pointer(&(tEnd[0]))

		cDimName := C.CString(dimName)
		defer C.free(unsafe.Pointer(cDimName))

		ret := C.tiledb_query_add_range_var_by_name(
			q.context.tiledbContext, q.tiledbQuery, cDimName, startBuffer,
			(C.uint64_t)(startSize), endBuffer, (C.uint64_t)(endSize))

		if ret != C.TILEDB_OK {
			return fmt.Errorf(
				"Error adding query range var: %s", q.context.LastError())
		}
	case reflect.Uint16:
		return fmt.Errorf("Unsupported type of range component passed: %s",
			startType.String())
	case reflect.Uint32:
		return fmt.Errorf("Unsupported type of range component passed: %s",
			startType.String())
	case reflect.Uint64:
		return fmt.Errorf("Unsupported type of range component passed: %s",
			startType.String())
	case reflect.Float32:
		return fmt.Errorf("Unsupported type of range component passed: %s",
			startType.String())
	case reflect.Float64:
		return fmt.Errorf("Unsupported type of range component passed: %s",
			startType.String())
	default:
		return fmt.Errorf("Unrecognized type of range component passed: %s",
			startType.String())
	}

	return nil
}

// GetRange retrieves a specific range of the query subarray
// along a given dimension.
// Returns (start, end, error)
// If start size or end size is 0 returns nil, nil, nil
// Stride is not supported at the moment, always nil
func (q *Query) GetRange(dimIdx uint32, rangeNum uint64) (interface{}, interface{}, error) {
	var pStart, pEnd, pStride unsafe.Pointer

	// Based on the type we fill in the interface{} objects for start, end
	var start, end interface{}

	// We need to infer the datatype of the dimension represented by index
	// dimIdx. That said:
	// Get array schema
	schema, err := q.array.Schema()
	if err != nil {
		return nil, nil, err
	}

	// Get the domain object
	domain, err := schema.Domain()
	if err != nil {
		return nil, nil, err
	}

	// Use the index to retrieve the dimension object
	dimension, err := domain.DimensionFromIndex(uint(dimIdx))
	if err != nil {
		return nil, nil, err
	}

	// Finally get the dimension's type
	datatype, err := dimension.Type()
	if err != nil {
		return nil, nil, err
	}

	cellValNum, err := dimension.CellValNum()
	if err != nil {
		return nil, nil, err
	}

	if cellValNum == TILEDB_VAR_NUM {

		var startSize, endSize C.uint64_t

		ret := C.tiledb_query_get_range_var_size(
			q.context.tiledbContext, q.tiledbQuery,
			(C.uint32_t)(dimIdx), (C.uint64_t)(rangeNum), &startSize, &endSize)

		if ret != C.TILEDB_OK {
			return nil, nil, fmt.Errorf(
				"Error retrieving query range: %s", q.context.LastError())
		}

		if startSize == 0 || endSize == 0 {
			return nil, nil, nil
		}

		startData := make([]byte, startSize)
		endData := make([]byte, endSize)

		ret = C.tiledb_query_get_range_var(
			q.context.tiledbContext, q.tiledbQuery,
			(C.uint32_t)(dimIdx), (C.uint64_t)(rangeNum), unsafe.Pointer(&startData[0]), unsafe.Pointer(&endData[0]))

		if ret != C.TILEDB_OK {
			return nil, nil, fmt.Errorf(
				"Error retrieving query range: %s", q.context.LastError())
		}

		start = startData
		end = endData

	} else {
		ret := C.tiledb_query_get_range(
			q.context.tiledbContext, q.tiledbQuery,
			(C.uint32_t)(dimIdx), (C.uint64_t)(rangeNum), &pStart, &pEnd, &pStride)

		if ret != C.TILEDB_OK {
			return nil, nil, fmt.Errorf(
				"Error retrieving query range: %s", q.context.LastError())
		}

		switch datatype {
		case TILEDB_INT8:
			start = *(*int8)(unsafe.Pointer(pStart))
			end = *(*int8)(unsafe.Pointer(pEnd))
		case TILEDB_INT16:
			start = *(*int16)(unsafe.Pointer(pStart))
			end = *(*int16)(unsafe.Pointer(pEnd))
		case TILEDB_INT32:
			start = *(*int32)(unsafe.Pointer(pStart))
			end = *(*int32)(unsafe.Pointer(pEnd))
		case TILEDB_INT64, TILEDB_DATETIME_YEAR, TILEDB_DATETIME_MONTH, TILEDB_DATETIME_WEEK, TILEDB_DATETIME_DAY, TILEDB_DATETIME_HR, TILEDB_DATETIME_MIN, TILEDB_DATETIME_SEC, TILEDB_DATETIME_MS, TILEDB_DATETIME_US, TILEDB_DATETIME_NS, TILEDB_DATETIME_PS, TILEDB_DATETIME_FS, TILEDB_DATETIME_AS, TILEDB_TIME_HR, TILEDB_TIME_MIN, TILEDB_TIME_SEC, TILEDB_TIME_MS, TILEDB_TIME_US, TILEDB_TIME_NS, TILEDB_TIME_PS, TILEDB_TIME_FS, TILEDB_TIME_AS:
			start = *(*int64)(unsafe.Pointer(pStart))
			end = *(*int64)(unsafe.Pointer(pEnd))
		case TILEDB_UINT8:
			start = *(*uint8)(unsafe.Pointer(pStart))
			end = *(*uint8)(unsafe.Pointer(pEnd))
		case TILEDB_UINT16:
			start = *(*uint16)(unsafe.Pointer(pStart))
			end = *(*uint16)(unsafe.Pointer(pEnd))
		case TILEDB_UINT32:
			start = *(*uint32)(unsafe.Pointer(pStart))
			end = *(*uint32)(unsafe.Pointer(pEnd))
		case TILEDB_UINT64:
			start = *(*uint64)(unsafe.Pointer(pStart))
			end = *(*uint64)(unsafe.Pointer(pEnd))
		case TILEDB_FLOAT32:
			start = *(*float32)(unsafe.Pointer(pStart))
			end = *(*float32)(unsafe.Pointer(pEnd))
		case TILEDB_FLOAT64:
			start = *(*float64)(unsafe.Pointer(pStart))
			end = *(*float64)(unsafe.Pointer(pEnd))
		case TILEDB_STRING_ASCII:
			start = *(*uint8)(unsafe.Pointer(pStart))
			end = *(*uint8)(unsafe.Pointer(pEnd))
		default:
			return nil, nil, fmt.Errorf("Unrecognized dimension type: %d", datatype)
		}
	}

	return start, end, nil
}

// GetRangeFromName retrieves a specific range of the query subarray
// along a given dimension.
// Returns (start, end, error)
// If start size or end size is 0 returns nil, nil, nil
// Stride is not supported at the moment, always nil
func (q *Query) GetRangeFromName(dimName string, rangeNum uint64) (interface{}, interface{}, error) {
	var pStart, pEnd, pStride unsafe.Pointer

	// Based on the type we fill in the interface{} objects for start, end
	var start, end interface{}

	// We need to infer the datatype of the dimension represented by index
	// dimIdx. That said:
	// Get array schema
	schema, err := q.array.Schema()
	if err != nil {
		return nil, nil, err
	}

	// Get the domain object
	domain, err := schema.Domain()
	if err != nil {
		return nil, nil, err
	}

	// Use the index to retrieve the dimension object
	dimension, err := domain.DimensionFromName((dimName))
	if err != nil {
		return nil, nil, err
	}

	// Finally get the dimension's type
	datatype, err := dimension.Type()
	if err != nil {
		return nil, nil, err
	}

	cellValNum, err := dimension.CellValNum()
	if err != nil {
		return nil, nil, err
	}

	cDimName := C.CString(dimName)
	defer C.free(unsafe.Pointer(cDimName))

	if cellValNum == TILEDB_VAR_NUM {

		var startSize, endSize C.uint64_t

		ret := C.tiledb_query_get_range_var_size_from_name(
			q.context.tiledbContext, q.tiledbQuery, cDimName,
			(C.uint64_t)(rangeNum), &startSize, &endSize)

		if ret != C.TILEDB_OK {
			return nil, nil, fmt.Errorf(
				"Error retrieving query range: %s", q.context.LastError())
		}

		if startSize == 0 || endSize == 0 {
			return nil, nil, nil
		}

		startData := make([]byte, startSize)
		endData := make([]byte, endSize)

		ret = C.tiledb_query_get_range_var_from_name(
			q.context.tiledbContext, q.tiledbQuery, cDimName,
			(C.uint64_t)(rangeNum), unsafe.Pointer(&startData[0]), unsafe.Pointer(&endData[0]))

		if ret != C.TILEDB_OK {
			return nil, nil, fmt.Errorf(
				"Error retrieving query range: %s", q.context.LastError())
		}

		start = startData
		end = endData

	} else {
		cDimName := C.CString(dimName)
		defer C.free(unsafe.Pointer(cDimName))

		ret := C.tiledb_query_get_range_from_name(
			q.context.tiledbContext, q.tiledbQuery, cDimName,
			(C.uint64_t)(rangeNum), &pStart, &pEnd, &pStride)

		if ret != C.TILEDB_OK {
			return nil, nil, fmt.Errorf(
				"Error retrieving query range: %s", q.context.LastError())
		}

		switch datatype {
		case TILEDB_INT8:
			start = *(*int8)(unsafe.Pointer(pStart))
			end = *(*int8)(unsafe.Pointer(pEnd))
		case TILEDB_INT16:
			start = *(*int16)(unsafe.Pointer(pStart))
			end = *(*int16)(unsafe.Pointer(pEnd))
		case TILEDB_INT32:
			start = *(*int32)(unsafe.Pointer(pStart))
			end = *(*int32)(unsafe.Pointer(pEnd))
		case TILEDB_INT64, TILEDB_DATETIME_YEAR, TILEDB_DATETIME_MONTH, TILEDB_DATETIME_WEEK, TILEDB_DATETIME_DAY, TILEDB_DATETIME_HR, TILEDB_DATETIME_MIN, TILEDB_DATETIME_SEC, TILEDB_DATETIME_MS, TILEDB_DATETIME_US, TILEDB_DATETIME_NS, TILEDB_DATETIME_PS, TILEDB_DATETIME_FS, TILEDB_DATETIME_AS, TILEDB_TIME_HR, TILEDB_TIME_MIN, TILEDB_TIME_SEC, TILEDB_TIME_MS, TILEDB_TIME_US, TILEDB_TIME_NS, TILEDB_TIME_PS, TILEDB_TIME_FS, TILEDB_TIME_AS:
			start = *(*int64)(unsafe.Pointer(pStart))
			end = *(*int64)(unsafe.Pointer(pEnd))
		case TILEDB_UINT8:
			start = *(*uint8)(unsafe.Pointer(pStart))
			end = *(*uint8)(unsafe.Pointer(pEnd))
		case TILEDB_UINT16:
			start = *(*uint16)(unsafe.Pointer(pStart))
			end = *(*uint16)(unsafe.Pointer(pEnd))
		case TILEDB_UINT32:
			start = *(*uint32)(unsafe.Pointer(pStart))
			end = *(*uint32)(unsafe.Pointer(pEnd))
		case TILEDB_UINT64:
			start = *(*uint64)(unsafe.Pointer(pStart))
			end = *(*uint64)(unsafe.Pointer(pEnd))
		case TILEDB_FLOAT32:
			start = *(*float32)(unsafe.Pointer(pStart))
			end = *(*float32)(unsafe.Pointer(pEnd))
		case TILEDB_FLOAT64:
			start = *(*float64)(unsafe.Pointer(pStart))
			end = *(*float64)(unsafe.Pointer(pEnd))
		case TILEDB_STRING_ASCII:
			start = *(*uint8)(unsafe.Pointer(pStart))
			end = *(*uint8)(unsafe.Pointer(pEnd))
		default:
			return nil, nil, fmt.Errorf("Unrecognized dimension type: %d", datatype)
		}
	}

	return start, end, nil
}

// GetRangeVar exists for continuinity with other TileDB APIs
// GetRange in Golang supports the variable length attribute also
// The function retrieves a specific range of the query subarray
// along a given dimension.
// Returns (start, end, error)
func (q *Query) GetRangeVar(dimIdx uint32, rangeNum uint64) (interface{}, interface{}, error) {
	return q.GetRange(dimIdx, rangeNum)
}

// GetRangeVarFromName exists for continuinity with other TileDB APIs
// GetRange in Golang supports the variable length attribute also
// The function retrieves a specific range of the query subarray
// along a given dimension.
// Returns (start, end, error)
func (q *Query) GetRangeVarFromName(dimName string, rangeNum uint64) (interface{}, interface{}, error) {
	return q.GetRangeFromName(dimName, rangeNum)
}

// GetRanges gets the number of dimensions from the array under current query
// and builds an array of dimensions that have as memmbers arrays of ranges
func (q *Query) GetRanges() (map[string][]RangeLimits, error) {
	// We need to infer the datatype of the dimension represented by index
	// dimIdx. That said:
	// Get array schema
	schema, err := q.array.Schema()
	if err != nil {
		return nil, err
	}

	// Get the domain object
	domain, err := schema.Domain()
	if err != nil {
		return nil, err
	}

	// Use the index to retrieve the dimension object
	nDim, err := domain.NDim()
	if err != nil {
		return nil, err
	}

	var dimIdx uint

	rangeMap := make(map[string][]RangeLimits)
	for dimIdx = 0; dimIdx < nDim; dimIdx++ {
		// Get dimension object
		dimension, err := domain.DimensionFromIndex(dimIdx)
		if err != nil {
			return nil, err
		}

		// Get name from dimension
		name, err := dimension.Name()
		if err != nil {
			return nil, err
		}

		// Get number of renges to iterate
		numOfRanges, err := q.GetRangeNum(uint32(dimIdx))
		if err != nil {
			return nil, err
		}

		var I uint64
		rangeArray := make([]RangeLimits, 0)
		for I = 0; I < *numOfRanges; I++ {

			start, end, err := q.GetRange(uint32(dimIdx), I)
			if err != nil {
				return nil, err
			}
			// Append range to range Array
			rangeArray = append(rangeArray, RangeLimits{start: start, end: end})
		}
		// key: name (string), value: rangeArray ([]RangeLimits)
		rangeMap[name] = rangeArray
	}

	return rangeMap, err
}

// GetRangeNum retrieves the number of ranges of the query subarray
// along a given dimension.
func (q *Query) GetRangeNum(dimIdx uint32) (*uint64, error) {
	var rangeNum uint64

	ret := C.tiledb_query_get_range_num(
		q.context.tiledbContext, q.tiledbQuery,
		(C.uint32_t)(dimIdx), (*C.uint64_t)(unsafe.Pointer(&rangeNum)))

	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf(
			"Error retrieving query range num: %s", q.context.LastError())
	}

	return &rangeNum, nil
}

// GetRangeNumFromName retrieves the number of ranges of the query subarray
// along a given dimension.
func (q *Query) GetRangeNumFromName(dimName string) (*uint64, error) {
	var rangeNum uint64

	cDimName := C.CString(dimName)
	defer C.free(unsafe.Pointer(cDimName))

	ret := C.tiledb_query_get_range_num_from_name(
		q.context.tiledbContext, q.tiledbQuery, cDimName,
		(*C.uint64_t)(unsafe.Pointer(&rangeNum)))

	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf(
			"Error retrieving query range num: %s", q.context.LastError())
	}

	return &rangeNum, nil
}

// Buffer returns a slice backed by the underlying c buffer from tiledb
func (q *Query) Buffer(attributeOrDimension string) (interface{}, error) {
	var datatype Datatype
	schema, err := q.array.Schema()
	if err != nil {
		return nil, err
	}

	domain, err := schema.Domain()
	if err != nil {
		return nil, fmt.Errorf(
			"Could not get domain from array schema for Buffer: %s",
			err)
	}

	if attributeOrDimension == TILEDB_COORDS {
		datatype, err = domain.Type()
		if err != nil {
			return nil, err
		}
	} else {
		hasDim, err := domain.HasDimension(attributeOrDimension)
		if err != nil {
			return nil, err
		}

		if hasDim {
			dimension, err := domain.DimensionFromName(attributeOrDimension)
			if err != nil {
				return nil, fmt.Errorf("Could not get attribute or dimension for SetBuffer: %s", attributeOrDimension)
			}

			datatype, err = dimension.Type()
			if err != nil {
				return nil, fmt.Errorf("Could not get dimensionType for SetBuffer: %s", attributeOrDimension)
			}
		} else {
			attribute, err := schema.AttributeFromName(attributeOrDimension)
			if err != nil {
				return nil, fmt.Errorf("Could not get attribute %s for Buffer", attributeOrDimension)
			}

			datatype, err = attribute.Type()
			if err != nil {
				return nil, fmt.Errorf("Could not get attributeType for SetBuffer: %s", attributeOrDimension)
			}
		}
	}

	cAttributeOrDimension := C.CString(attributeOrDimension)
	defer C.free(unsafe.Pointer(cAttributeOrDimension))

	var ret C.int32_t
	var cbufferSize *C.uint64_t
	var cbuffer unsafe.Pointer
	var buffer interface{}
	switch datatype {
	case TILEDB_INT8:
		ret = C.tiledb_query_get_buffer(q.context.tiledbContext, q.tiledbQuery, cAttributeOrDimension, &cbuffer, &cbufferSize)
		length := (*cbufferSize) / C.sizeof_int8_t
		buffer = (*[1 << 46]int8)(cbuffer)[:length:length]

	case TILEDB_INT16:
		ret = C.tiledb_query_get_buffer(q.context.tiledbContext, q.tiledbQuery, cAttributeOrDimension, &cbuffer, &cbufferSize)
		length := (*cbufferSize) / C.sizeof_int16_t
		buffer = (*[1 << 46]int16)(cbuffer)[:length:length]

	case TILEDB_INT32:
		ret = C.tiledb_query_get_buffer(q.context.tiledbContext, q.tiledbQuery, cAttributeOrDimension, &cbuffer, &cbufferSize)
		length := (*cbufferSize) / C.sizeof_int32_t
		buffer = (*[1 << 46]int32)(cbuffer)[:length:length]

	case TILEDB_INT64, TILEDB_DATETIME_YEAR, TILEDB_DATETIME_MONTH, TILEDB_DATETIME_WEEK, TILEDB_DATETIME_DAY, TILEDB_DATETIME_HR, TILEDB_DATETIME_MIN, TILEDB_DATETIME_SEC, TILEDB_DATETIME_MS, TILEDB_DATETIME_US, TILEDB_DATETIME_NS, TILEDB_DATETIME_PS, TILEDB_DATETIME_FS, TILEDB_DATETIME_AS, TILEDB_TIME_HR, TILEDB_TIME_MIN, TILEDB_TIME_SEC, TILEDB_TIME_MS, TILEDB_TIME_US, TILEDB_TIME_NS, TILEDB_TIME_PS, TILEDB_TIME_FS, TILEDB_TIME_AS:
		ret = C.tiledb_query_get_buffer(q.context.tiledbContext, q.tiledbQuery, cAttributeOrDimension, &cbuffer, &cbufferSize)
		length := (*cbufferSize) / C.sizeof_int64_t
		buffer = (*[1 << 46]int64)(cbuffer)[:length:length]

	case TILEDB_UINT8:
		ret = C.tiledb_query_get_buffer(q.context.tiledbContext, q.tiledbQuery, cAttributeOrDimension, &cbuffer, &cbufferSize)
		length := (*cbufferSize) / C.sizeof_uint8_t
		buffer = (*[1 << 46]uint8)(cbuffer)[:length:length]

	case TILEDB_UINT16:
		ret = C.tiledb_query_get_buffer(q.context.tiledbContext, q.tiledbQuery, cAttributeOrDimension, &cbuffer, &cbufferSize)
		length := (*cbufferSize) / C.sizeof_uint16_t
		buffer = (*[1 << 46]uint16)(cbuffer)[:length:length]

	case TILEDB_UINT32:
		ret = C.tiledb_query_get_buffer(q.context.tiledbContext, q.tiledbQuery, cAttributeOrDimension, &cbuffer, &cbufferSize)
		length := (*cbufferSize) / C.sizeof_uint32_t
		buffer = (*[1 << 46]uint32)(cbuffer)[:length:length]

	case TILEDB_UINT64:
		ret = C.tiledb_query_get_buffer(q.context.tiledbContext, q.tiledbQuery, cAttributeOrDimension, &cbuffer, &cbufferSize)
		length := (*cbufferSize) / C.sizeof_uint64_t
		buffer = (*[1 << 46]uint64)(cbuffer)[:length:length]

	case TILEDB_FLOAT32:
		ret = C.tiledb_query_get_buffer(q.context.tiledbContext, q.tiledbQuery, cAttributeOrDimension, &cbuffer, &cbufferSize)
		length := (*cbufferSize) / C.sizeof_float
		buffer = (*[1 << 46]float32)(cbuffer)[:length:length]

	case TILEDB_FLOAT64:
		ret = C.tiledb_query_get_buffer(q.context.tiledbContext, q.tiledbQuery, cAttributeOrDimension, &cbuffer, &cbufferSize)
		length := (*cbufferSize) / C.sizeof_double
		buffer = (*[1 << 46]float64)(cbuffer)[:length:length]

	case TILEDB_CHAR:
		ret = C.tiledb_query_get_buffer(q.context.tiledbContext, q.tiledbQuery, cAttributeOrDimension, &cbuffer, &cbufferSize)
		length := (*cbufferSize) / C.sizeof_char
		buffer = (*[1 << 46]byte)(cbuffer)[:length:length]

	case TILEDB_STRING_ASCII:
		ret = C.tiledb_query_get_buffer(q.context.tiledbContext, q.tiledbQuery, cAttributeOrDimension, &cbuffer, &cbufferSize)
		length := (*cbufferSize) / C.sizeof_uint8_t
		buffer = (*[1 << 46]uint8)(cbuffer)[:length:length]

	case TILEDB_STRING_UTF8:
		ret = C.tiledb_query_get_buffer(q.context.tiledbContext, q.tiledbQuery, cAttributeOrDimension, &cbuffer, &cbufferSize)
		length := (*cbufferSize) / C.sizeof_uint8_t
		buffer = (*[1 << 46]uint8)(cbuffer)[:length:length]

	case TILEDB_STRING_UTF16:
		ret = C.tiledb_query_get_buffer(q.context.tiledbContext, q.tiledbQuery, cAttributeOrDimension, &cbuffer, &cbufferSize)
		length := (*cbufferSize) / C.sizeof_uint16_t
		buffer = (*[1 << 46]uint16)(cbuffer)[:length:length]

	case TILEDB_STRING_UTF32:
		ret = C.tiledb_query_get_buffer(q.context.tiledbContext, q.tiledbQuery, cAttributeOrDimension, &cbuffer, &cbufferSize)
		length := (*cbufferSize) / C.sizeof_uint32_t
		buffer = (*[1 << 46]uint32)(cbuffer)[:length:length]

	case TILEDB_STRING_UCS2:
		ret = C.tiledb_query_get_buffer(q.context.tiledbContext, q.tiledbQuery, cAttributeOrDimension, &cbuffer, &cbufferSize)
		length := (*cbufferSize) / C.sizeof_uint16_t
		buffer = (*[1 << 46]uint16)(cbuffer)[:length:length]

	case TILEDB_STRING_UCS4:
		ret = C.tiledb_query_get_buffer(q.context.tiledbContext, q.tiledbQuery, cAttributeOrDimension, &cbuffer, &cbufferSize)
		length := (*cbufferSize) / C.sizeof_uint32_t
		buffer = (*[1 << 46]uint32)(cbuffer)[:length:length]

	case TILEDB_ANY:
		ret = C.tiledb_query_get_buffer(q.context.tiledbContext, q.tiledbQuery, cAttributeOrDimension, &cbuffer, &cbufferSize)
		length := (*cbufferSize) / C.sizeof_int32_t
		buffer = (*[1 << 46]C.int8_t)(cbuffer)[:length:length]

	default:
		return nil, fmt.Errorf("Unrecognized attribute type: %d", datatype)
	}
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error getting tiledb query buffer for %s: %s", attributeOrDimension, q.context.LastError())
	}

	return buffer, nil
}

// BufferNullable returns a slice backed by the underlying c buffer from tiledb for
// validities, and values
func (q *Query) BufferNullable(attributeOrDimension string) (interface{}, []uint8, error) {
	schema, err := q.array.Schema()
	if err != nil {
		return nil, nil, err
	}

	domain, err := schema.Domain()
	if err != nil {
		return nil, nil, fmt.Errorf("Could not get domain from array schema for BufferNullable: %s", err)
	}

	var datatype Datatype
	if attributeOrDimension == TILEDB_COORDS {
		datatype, err = domain.Type()
		if err != nil {
			return nil, nil, err
		}
	} else {
		hasDim, err := domain.HasDimension(attributeOrDimension)
		if err != nil {
			return nil, nil, err
		}

		if hasDim {
			dimension, err := domain.DimensionFromName(attributeOrDimension)
			if err != nil {
				return nil, nil, fmt.Errorf("Could not get attribute or dimension for BufferNullable: %s", attributeOrDimension)
			}

			datatype, err = dimension.Type()
			if err != nil {
				return nil, nil, fmt.Errorf("Could not get dimensionType for BufferNullable: %s", attributeOrDimension)
			}
		} else {
			attribute, err := schema.AttributeFromName(attributeOrDimension)
			if err != nil {
				return nil, nil, fmt.Errorf("Could not get attribute for BufferNullable: %s", attributeOrDimension)
			}

			datatype, err = attribute.Type()
			if err != nil {
				return nil, nil, fmt.Errorf("Could not get attributeType for BufferNullable: %s", attributeOrDimension)
			}
		}
	}

	cattributeNameOrDimension := C.CString(attributeOrDimension)
	defer C.free(unsafe.Pointer(cattributeNameOrDimension))

	var ret C.int32_t
	var cbuffer unsafe.Pointer
	var cbufferSize, cvalidityByteMapSize *C.uint64_t
	var cvalidityByteMap *C.uint8_t

	ret = C.tiledb_query_get_buffer_nullable(q.context.tiledbContext, q.tiledbQuery, cattributeNameOrDimension, &cbuffer, &cbufferSize, &cvalidityByteMap, &cvalidityByteMapSize)
	if ret != C.TILEDB_OK {
		return nil, nil, fmt.Errorf("Error getting tiledb query buffer for %s: %s", attributeOrDimension, q.context.LastError())
	}

	// build validities
	validityByteMapLength := *cvalidityByteMapSize / C.sizeof_uint8_t
	validities := (*[1 << 46]uint8)(unsafe.Pointer(cvalidityByteMap))[:validityByteMapLength:validityByteMapLength]

	// build buffer
	var buffer interface{}
	switch datatype {
	case TILEDB_INT8:
		length := (*cbufferSize) / C.sizeof_int8_t
		buffer = (*[1 << 46]int8)(cbuffer)[:length:length]

	case TILEDB_INT16:
		length := (*cbufferSize) / C.sizeof_int16_t
		buffer = (*[1 << 46]int16)(cbuffer)[:length:length]

	case TILEDB_INT32:
		length := (*cbufferSize) / C.sizeof_int32_t
		buffer = (*[1 << 46]int32)(cbuffer)[:length:length]

	case TILEDB_INT64, TILEDB_DATETIME_YEAR, TILEDB_DATETIME_MONTH, TILEDB_DATETIME_WEEK, TILEDB_DATETIME_DAY, TILEDB_DATETIME_HR, TILEDB_DATETIME_MIN, TILEDB_DATETIME_SEC, TILEDB_DATETIME_MS, TILEDB_DATETIME_US, TILEDB_DATETIME_NS, TILEDB_DATETIME_PS, TILEDB_DATETIME_FS, TILEDB_DATETIME_AS, TILEDB_TIME_HR, TILEDB_TIME_MIN, TILEDB_TIME_SEC, TILEDB_TIME_MS, TILEDB_TIME_US, TILEDB_TIME_NS, TILEDB_TIME_PS, TILEDB_TIME_FS, TILEDB_TIME_AS:
		length := (*cbufferSize) / C.sizeof_int64_t
		buffer = (*[1 << 46]int64)(cbuffer)[:length:length]

	case TILEDB_UINT8:
		length := (*cbufferSize) / C.sizeof_uint8_t
		buffer = (*[1 << 46]uint8)(cbuffer)[:length:length]

	case TILEDB_UINT16:
		length := (*cbufferSize) / C.sizeof_uint16_t
		buffer = (*[1 << 46]uint16)(cbuffer)[:length:length]

	case TILEDB_UINT32:
		length := (*cbufferSize) / C.sizeof_uint32_t
		buffer = (*[1 << 46]uint32)(cbuffer)[:length:length]

	case TILEDB_UINT64:
		length := (*cbufferSize) / C.sizeof_uint64_t
		buffer = (*[1 << 46]uint64)(cbuffer)[:length:length]

	case TILEDB_FLOAT32:
		length := (*cbufferSize) / C.sizeof_float
		buffer = (*[1 << 46]float32)(cbuffer)[:length:length]

	case TILEDB_FLOAT64:
		length := (*cbufferSize) / C.sizeof_double
		buffer = (*[1 << 46]float64)(cbuffer)[:length:length]

	case TILEDB_CHAR:
		length := (*cbufferSize) / C.sizeof_char
		buffer = (*[1 << 46]byte)(cbuffer)[:length:length]

	case TILEDB_STRING_ASCII:
		length := (*cbufferSize) / C.sizeof_uint8_t
		buffer = (*[1 << 46]uint8)(cbuffer)[:length:length]

	case TILEDB_STRING_UTF8:
		length := (*cbufferSize) / C.sizeof_uint8_t
		buffer = (*[1 << 46]uint8)(cbuffer)[:length:length]

	case TILEDB_STRING_UTF16:
		length := (*cbufferSize) / C.sizeof_uint16_t
		buffer = (*[1 << 46]uint16)(cbuffer)[:length:length]

	case TILEDB_STRING_UTF32:
		length := (*cbufferSize) / C.sizeof_uint32_t
		buffer = (*[1 << 46]uint32)(cbuffer)[:length:length]

	case TILEDB_STRING_UCS2:
		length := (*cbufferSize) / C.sizeof_uint16_t
		buffer = (*[1 << 46]uint16)(cbuffer)[:length:length]

	case TILEDB_STRING_UCS4:
		length := (*cbufferSize) / C.sizeof_uint32_t
		buffer = (*[1 << 46]uint32)(cbuffer)[:length:length]

	case TILEDB_ANY:
		length := (*cbufferSize) / C.sizeof_int32_t
		buffer = (*[1 << 46]C.int8_t)(cbuffer)[:length:length]

	default:
		return nil, nil, fmt.Errorf("Unrecognized attribute type: %d", datatype)
	}

	return buffer, validities, nil
}

// SetBufferVarUnsafe Sets the buffer for a variable sized attribute to a query
// This takes unsafe pointers which is passsed straight to tiledb c_api
// for advanced usage
func (q *Query) SetBufferVarUnsafe(attribute string, offset unsafe.Pointer, offsetSize uint64, buffer unsafe.Pointer, bufferSize uint64) (*uint64, *uint64, error) {
	cAttribute := C.CString(attribute)
	defer C.free(unsafe.Pointer(cAttribute))

	ret := C.tiledb_query_set_buffer_var(
		q.context.tiledbContext,
		q.tiledbQuery,
		cAttribute,
		(*C.uint64_t)(offset),
		(*C.uint64_t)(unsafe.Pointer(&offsetSize)),
		buffer,
		(*C.uint64_t)(unsafe.Pointer(&bufferSize)))

	if ret != C.TILEDB_OK {
		return nil, nil, fmt.Errorf("Error setting query var buffer: %s", q.context.LastError())
	}

	q.resultBufferElements[attribute] = [3]*uint64{&offsetSize, &bufferSize, nil}

	return &offsetSize, &bufferSize, nil
}

// SetBufferVar Sets the buffer for a variable sized attribute/dimension to a query
// The buffer must be an initialized slice
func (q *Query) SetBufferVar(attributeOrDimension string, offset []uint64, buffer interface{}) (*uint64, *uint64, error) {
	bufferReflectType := reflect.TypeOf(buffer)
	bufferReflectValue := reflect.ValueOf(buffer)
	if bufferReflectValue.Kind() != reflect.Slice {
		return nil, nil, fmt.Errorf("Buffer passed must be a slice that is pre"+
			"-allocated, type passed was: %s", bufferReflectValue.Kind().String())
	}

	// Next get the attribute to validate the buffer type is the same as the attribute
	schema, err := q.array.Schema()
	if err != nil {
		return nil, nil, fmt.Errorf(
			"Could not get array schema for SetBuffer: %s",
			err)
	}

	var attributeOrDimensionType Datatype

	domain, err := schema.Domain()
	if err != nil {
		return nil, nil, fmt.Errorf(
			"Could not get domain from array schema for SetBufferVar: %s",
			err)
	}

	hasDim, err := domain.HasDimension(attributeOrDimension)
	if err != nil {
		return nil, nil, err
	}

	if hasDim {
		dimension, err := domain.DimensionFromName(attributeOrDimension)
		if err != nil {
			return nil, nil, fmt.Errorf("Could not get attribute or dimension for SetBufferVar: %s",
				attributeOrDimension)
		}
		attributeOrDimensionType, err = dimension.Type()
		if err != nil {
			return nil, nil, fmt.Errorf("Could not get dimensionType for SetBufferVar: %s",
				attributeOrDimension)
		}
	} else {
		schemaAttribute, err := schema.AttributeFromName(attributeOrDimension)
		if err != nil {
			return nil, nil, fmt.Errorf("Could not get attribute %s SetBufferVar",
				attributeOrDimension)
		}

		attributeOrDimensionType, err = schemaAttribute.Type()
		if err != nil {
			return nil, nil, fmt.Errorf("Could not get attributeType for SetBufferVar: %s",
				attributeOrDimension)
		}
	}

	bufferType := bufferReflectType.Elem().Kind()

	if attributeOrDimensionType.ReflectKind() != bufferType {
		return nil, nil, fmt.Errorf("Buffer and Attribute do not have the same"+
			" data types. Buffer: %s, Attribute: %s", bufferType.String(), attributeOrDimensionType.ReflectKind().String())
	}

	bufferSize := uint64(bufferReflectValue.Len())

	if bufferSize == uint64(0) {
		return nil, nil, fmt.Errorf("Buffer has no length, " +
			"buffers are required to be initialized before reading or writting")
	}

	offsetSize := uint64(len(offset)) * bytesizes.Uint64

	if offsetSize == uint64(0) {
		return nil, nil, fmt.Errorf("Offset slice has no length, " +
			"offset slices are required to be initialized before reading or writting")
	}

	// Acquire a lock to make appending to buffer slice thread safe
	q.bufferMutex.Lock()
	defer q.bufferMutex.Unlock()

	// Store offset so array does not get gc'ed
	q.buffers = append(q.buffers, offset)

	// Set offset and buffer
	var cbuffer unsafe.Pointer
	coffset := unsafe.Pointer(&(offset)[0])
	switch bufferType {
	case reflect.Int:
		// Set buffersize
		bufferSize = bufferSize * bytesizes.Int

		// Create buffer void*
		tmpBuffer := buffer.([]int)
		// Store slice so underlying array is not gc'ed
		q.buffers = append(q.buffers, tmpBuffer)
		cbuffer = unsafe.Pointer(&(tmpBuffer)[0])
	case reflect.Int8:
		// Set buffersize
		bufferSize = bufferSize * bytesizes.Int8

		// Create buffer void*
		tmpBuffer := buffer.([]int8)
		// Store slice so underlying array is not gc'ed
		q.buffers = append(q.buffers, tmpBuffer)
		cbuffer = unsafe.Pointer(&(tmpBuffer)[0])
	case reflect.Int16:
		// Set buffersize
		bufferSize = bufferSize * bytesizes.Int16

		// Create buffer void*
		tmpBuffer := buffer.([]int16)
		// Store slice so underlying array is not gc'ed
		q.buffers = append(q.buffers, tmpBuffer)
		cbuffer = unsafe.Pointer(&(tmpBuffer)[0])
	case reflect.Int32:
		// Set buffersize
		bufferSize = bufferSize * bytesizes.Int32

		// Create buffer void*
		tmpBuffer := buffer.([]int32)
		// Store slice so underlying array is not gc'ed
		q.buffers = append(q.buffers, tmpBuffer)
		cbuffer = unsafe.Pointer(&(tmpBuffer)[0])
	case reflect.Int64:
		// Set buffersize
		bufferSize = bufferSize * bytesizes.Int64

		// Create buffer void*
		tmpBuffer := buffer.([]int64)
		// Store slice so underlying array is not gc'ed
		q.buffers = append(q.buffers, tmpBuffer)
		cbuffer = unsafe.Pointer(&(tmpBuffer)[0])
	case reflect.Uint:
		// Set buffersize
		bufferSize = bufferSize * bytesizes.Uint

		// Create buffer void*
		tmpBuffer := buffer.([]uint)
		// Store slice so underlying array is not gc'ed
		q.buffers = append(q.buffers, tmpBuffer)
		cbuffer = unsafe.Pointer(&(tmpBuffer)[0])
	case reflect.Uint8:
		// Set buffersize
		bufferSize = bufferSize * bytesizes.Uint8

		// Create buffer void*
		tmpBuffer := buffer.([]uint8)
		// Store slice so underlying array is not gc'ed
		q.buffers = append(q.buffers, tmpBuffer)
		cbuffer = unsafe.Pointer(&(tmpBuffer)[0])
	case reflect.Uint16:
		// Set buffersize
		bufferSize = bufferSize * bytesizes.Uint16

		// Create buffer void*
		tmpBuffer := buffer.([]uint16)
		// Store slice so underlying array is not gc'ed
		q.buffers = append(q.buffers, tmpBuffer)
		cbuffer = unsafe.Pointer(&(tmpBuffer)[0])
	case reflect.Uint32:
		// Set buffersize
		bufferSize = bufferSize * bytesizes.Uint32

		// Create buffer void*
		tmpBuffer := buffer.([]uint32)
		// Store slice so underlying array is not gc'ed
		q.buffers = append(q.buffers, tmpBuffer)
		cbuffer = unsafe.Pointer(&(tmpBuffer)[0])
	case reflect.Uint64:
		// Set buffersize
		bufferSize = bufferSize * bytesizes.Uint64

		// Create buffer void*
		tmpBuffer := buffer.([]uint64)
		// Store slice so underlying array is not gc'ed
		q.buffers = append(q.buffers, tmpBuffer)
		cbuffer = unsafe.Pointer(&(tmpBuffer)[0])
	case reflect.Float32:
		// Set buffersize
		bufferSize = bufferSize * bytesizes.Float32

		// Create buffer void*
		tmpBuffer := buffer.([]float32)
		// Store slice so underlying array is not gc'ed
		q.buffers = append(q.buffers, tmpBuffer)
		cbuffer = unsafe.Pointer(&(tmpBuffer)[0])
	case reflect.Float64:
		// Set buffersize
		bufferSize = bufferSize * bytesizes.Float64

		// Create buffer void*
		tmpBuffer := buffer.([]float64)
		// Store slice so underlying array is not gc'ed
		q.buffers = append(q.buffers, tmpBuffer)
		cbuffer = unsafe.Pointer(&(tmpBuffer)[0])
	default:
		return nil, nil, fmt.Errorf("Unrecognized buffer type passed: %s",
			bufferType.String())
	}

	cAttributeOrDimension := C.CString(attributeOrDimension)
	defer C.free(unsafe.Pointer(cAttributeOrDimension))

	ret := C.tiledb_query_set_buffer_var(
		q.context.tiledbContext,
		q.tiledbQuery,
		cAttributeOrDimension,
		(*C.uint64_t)(coffset),
		(*C.uint64_t)(unsafe.Pointer(&offsetSize)),
		cbuffer,
		(*C.uint64_t)(unsafe.Pointer(&bufferSize)))

	if ret != C.TILEDB_OK {
		return nil, nil, fmt.Errorf("Error setting query var buffer: %s",
			q.context.LastError())
	}

	q.resultBufferElements[attributeOrDimension] =
		[3]*uint64{&offsetSize, &bufferSize, nil}

	return &offsetSize, &bufferSize, nil
}

// SetBufferVarNullableUnsafe Sets the buffer for a variable sized nullable attribute to a query
// This takes unsafe pointers which is passsed straight to tiledb c_api
// for advanced usage
func (q *Query) SetBufferVarNullableUnsafe(attribute string, offset unsafe.Pointer, offsetSize uint64, buffer unsafe.Pointer, bufferSize uint64, bufferValidity unsafe.Pointer, bufferValiditySize uint64) (*uint64, *uint64, *uint64, error) {
	cAttribute := C.CString(attribute)
	defer C.free(unsafe.Pointer(cAttribute))

	ret := C.tiledb_query_set_buffer_var_nullable(
		q.context.tiledbContext,
		q.tiledbQuery,
		cAttribute,
		(*C.uint64_t)(offset),
		(*C.uint64_t)(unsafe.Pointer(&offsetSize)),
		buffer,
		(*C.uint64_t)(unsafe.Pointer(&bufferSize)),
		(*C.uint8_t)(bufferValidity),
		(*C.uint64_t)(unsafe.Pointer(&bufferValiditySize)),
	)

	if ret != C.TILEDB_OK {
		return nil, nil, nil, fmt.Errorf("Error setting query var buffer: %s", q.context.LastError())
	}

	q.resultBufferElements[attribute] = [3]*uint64{&offsetSize, &bufferSize, &bufferValiditySize}

	return &offsetSize, &bufferSize, &bufferValiditySize, nil
}

// SetBufferVarNullable Sets the buffer for a variable sized nullable attribute/dimension to a query
// The buffer must be an initialized slice
func (q *Query) SetBufferVarNullable(attributeOrDimension string, offset []uint64, buffer interface{}, bufferValidity []uint8) (*uint64, *uint64, *uint64, error) {
	bufferReflectType := reflect.TypeOf(buffer)
	bufferReflectValue := reflect.ValueOf(buffer)
	if bufferReflectValue.Kind() != reflect.Slice {
		return nil, nil, nil, fmt.Errorf("Buffer passed must be a slice that is pre"+
			"-allocated, type passed was: %s", bufferReflectValue.Kind().String())
	}

	// Next get the attribute to validate the buffer type is the same as the attribute
	schema, err := q.array.Schema()
	if err != nil {
		return nil, nil, nil, fmt.Errorf(
			"Could not get array schema for SetBuffer: %s",
			err)
	}

	var attributeOrDimensionType Datatype

	domain, err := schema.Domain()
	if err != nil {
		return nil, nil, nil, fmt.Errorf(
			"Could not get domain from array schema for SetBufferVar: %s",
			err)
	}

	hasDim, err := domain.HasDimension(attributeOrDimension)
	if err != nil {
		return nil, nil, nil, err
	}

	if hasDim {
		dimension, err := domain.DimensionFromName(attributeOrDimension)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("Could not get attribute or dimension for SetBufferVar: %s",
				attributeOrDimension)
		}
		attributeOrDimensionType, err = dimension.Type()
		if err != nil {
			return nil, nil, nil, fmt.Errorf("Could not get dimensionType for SetBufferVar: %s",
				attributeOrDimension)
		}
	} else {
		schemaAttribute, err := schema.AttributeFromName(attributeOrDimension)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("Could not get attribute %s SetBufferVar",
				attributeOrDimension)
		}

		attributeOrDimensionType, err = schemaAttribute.Type()
		if err != nil {
			return nil, nil, nil, fmt.Errorf("Could not get attributeType for SetBufferVar: %s",
				attributeOrDimension)
		}
	}

	bufferType := bufferReflectType.Elem().Kind()

	if attributeOrDimensionType.ReflectKind() != bufferType {
		return nil, nil, nil, fmt.Errorf("Buffer and Attribute do not have the same"+
			" data types. Buffer: %s, Attribute: %s", bufferType.String(), attributeOrDimensionType.ReflectKind().String())
	}

	bufferSize := uint64(bufferReflectValue.Len())

	if bufferSize == uint64(0) {
		return nil, nil, nil, fmt.Errorf("Buffer has no length, " +
			"buffers are required to be initialized before reading or writting")
	}

	offsetSize := uint64(len(offset)) * bytesizes.Uint64

	if offsetSize == uint64(0) {
		return nil, nil, nil, fmt.Errorf("Offset slice has no length, " +
			"offset slices are required to be initialized before reading or writting")
	}

	bufferValiditySize := uint64(len(bufferValidity)) * bytesizes.Uint8
	if bufferValiditySize == uint64(0) {
		return nil, nil, nil, fmt.Errorf("Validity slice has no length, " +
			"nullable slices are required to be initialized before reading or writting")
	}

	// Acquire a lock to make appending to buffer slice thread safe
	q.bufferMutex.Lock()
	defer q.bufferMutex.Unlock()

	// Store offset so array does not get gc'ed
	q.buffers = append(q.buffers, offset)

	// Set offset and buffer
	var cbuffer unsafe.Pointer
	coffset := unsafe.Pointer(&(offset)[0])
	cbufferValidity := unsafe.Pointer(&(bufferValidity)[0])
	switch bufferType {
	case reflect.Int:
		// Set buffersize
		bufferSize = bufferSize * bytesizes.Int

		// Create buffer void*
		tmpBuffer := buffer.([]int)
		// Store slice so underlying array is not gc'ed
		q.buffers = append(q.buffers, tmpBuffer)
		cbuffer = unsafe.Pointer(&(tmpBuffer)[0])
	case reflect.Int8:
		// Set buffersize
		bufferSize = bufferSize * bytesizes.Int8

		// Create buffer void*
		tmpBuffer := buffer.([]int8)
		// Store slice so underlying array is not gc'ed
		q.buffers = append(q.buffers, tmpBuffer)
		cbuffer = unsafe.Pointer(&(tmpBuffer)[0])
	case reflect.Int16:
		// Set buffersize
		bufferSize = bufferSize * bytesizes.Int16

		// Create buffer void*
		tmpBuffer := buffer.([]int16)
		// Store slice so underlying array is not gc'ed
		q.buffers = append(q.buffers, tmpBuffer)
		cbuffer = unsafe.Pointer(&(tmpBuffer)[0])
	case reflect.Int32:
		// Set buffersize
		bufferSize = bufferSize * bytesizes.Int32

		// Create buffer void*
		tmpBuffer := buffer.([]int32)
		// Store slice so underlying array is not gc'ed
		q.buffers = append(q.buffers, tmpBuffer)
		cbuffer = unsafe.Pointer(&(tmpBuffer)[0])
	case reflect.Int64:
		// Set buffersize
		bufferSize = bufferSize * bytesizes.Int64

		// Create buffer void*
		tmpBuffer := buffer.([]int64)
		// Store slice so underlying array is not gc'ed
		q.buffers = append(q.buffers, tmpBuffer)
		cbuffer = unsafe.Pointer(&(tmpBuffer)[0])
	case reflect.Uint:
		// Set buffersize
		bufferSize = bufferSize * bytesizes.Uint

		// Create buffer void*
		tmpBuffer := buffer.([]uint)
		// Store slice so underlying array is not gc'ed
		q.buffers = append(q.buffers, tmpBuffer)
		cbuffer = unsafe.Pointer(&(tmpBuffer)[0])
	case reflect.Uint8:
		// Set buffersize
		bufferSize = bufferSize * bytesizes.Uint8

		// Create buffer void*
		tmpBuffer := buffer.([]uint8)
		// Store slice so underlying array is not gc'ed
		q.buffers = append(q.buffers, tmpBuffer)
		cbuffer = unsafe.Pointer(&(tmpBuffer)[0])
	case reflect.Uint16:
		// Set buffersize
		bufferSize = bufferSize * bytesizes.Uint16

		// Create buffer void*
		tmpBuffer := buffer.([]uint16)
		// Store slice so underlying array is not gc'ed
		q.buffers = append(q.buffers, tmpBuffer)
		cbuffer = unsafe.Pointer(&(tmpBuffer)[0])
	case reflect.Uint32:
		// Set buffersize
		bufferSize = bufferSize * bytesizes.Uint32

		// Create buffer void*
		tmpBuffer := buffer.([]uint32)
		// Store slice so underlying array is not gc'ed
		q.buffers = append(q.buffers, tmpBuffer)
		cbuffer = unsafe.Pointer(&(tmpBuffer)[0])
	case reflect.Uint64:
		// Set buffersize
		bufferSize = bufferSize * bytesizes.Uint64

		// Create buffer void*
		tmpBuffer := buffer.([]uint64)
		// Store slice so underlying array is not gc'ed
		q.buffers = append(q.buffers, tmpBuffer)
		cbuffer = unsafe.Pointer(&(tmpBuffer)[0])
	case reflect.Float32:
		// Set buffersize
		bufferSize = bufferSize * bytesizes.Float32

		// Create buffer void*
		tmpBuffer := buffer.([]float32)
		// Store slice so underlying array is not gc'ed
		q.buffers = append(q.buffers, tmpBuffer)
		cbuffer = unsafe.Pointer(&(tmpBuffer)[0])
	case reflect.Float64:
		// Set buffersize
		bufferSize = bufferSize * bytesizes.Float64

		// Create buffer void*
		tmpBuffer := buffer.([]float64)
		// Store slice so underlying array is not gc'ed
		q.buffers = append(q.buffers, tmpBuffer)
		cbuffer = unsafe.Pointer(&(tmpBuffer)[0])
	default:
		return nil, nil, nil, fmt.Errorf("Unrecognized buffer type passed: %s",
			bufferType.String())
	}

	cAttributeOrDimension := C.CString(attributeOrDimension)
	defer C.free(unsafe.Pointer(cAttributeOrDimension))

	ret := C.tiledb_query_set_buffer_var_nullable(
		q.context.tiledbContext,
		q.tiledbQuery,
		cAttributeOrDimension,
		(*C.uint64_t)(coffset),
		(*C.uint64_t)(unsafe.Pointer(&offsetSize)),
		cbuffer,
		(*C.uint64_t)(unsafe.Pointer(&bufferSize)),
		(*C.uint8_t)(cbufferValidity),
		(*C.uint64_t)(unsafe.Pointer(&bufferValiditySize)))

	if ret != C.TILEDB_OK {
		return nil, nil, nil, fmt.Errorf("Error setting query var buffer: %s",
			q.context.LastError())
	}

	q.resultBufferElements[attributeOrDimension] =
		[3]*uint64{&offsetSize, &bufferSize, &bufferValiditySize}

	return &offsetSize, &bufferSize, &bufferValiditySize, nil
}

// ResultBufferElements returns the number of elements in the result buffers
// from a read query.
// This is a map from the attribute name to a pair of values.
// The first is number of elements (offsets) for var size attributes, and the
// second is number of elements in the data buffer. For fixed sized attributes
// (and coordinates), the first is always 0.
func (q *Query) ResultBufferElements() (map[string][3]uint64, error) {
	elements := make(map[string][3]uint64)

	// Will need the schema to infer data type size for attributes
	schema, err := q.array.Schema()
	if err != nil {
		return nil, fmt.Errorf("Could not get schema for ResultBufferElements: %s", err)
	}

	domain, err := schema.Domain()
	if err != nil {
		return nil, fmt.Errorf("Could not get domain for ResultBufferElements: %s", err)
	}

	var datatype Datatype
	for attributeOrDimension, v := range q.resultBufferElements {
		// Handle coordinates
		if attributeOrDimension == TILEDB_COORDS {
			// For fixed length attributes offset elements are always zero
			offsetElements := uint64(0)

			domainType, err := domain.Type()
			if err != nil {
				return nil, fmt.Errorf("Could not get domainType for ResultBufferElements: %s", err)
			}

			// Number of buffer elements is calculated
			bufferElements := (*v[1]) / domainType.Size()
			elements[attributeOrDimension] = [3]uint64{offsetElements, bufferElements, 0}
		} else {
			// For fixed length attributes offset elements are always zero
			offsetElements := uint64(0)
			if v[0] != nil {
				// The attribute is variable lenght
				offsetElements = (*v[0]) / bytesizes.Uint64
			}

			validityElements := uint64(0)
			if v[2] != nil {
				// The attribute is nullable
				validityElements = (*v[2]) / bytesizes.Uint8
			}

			hasDim, err := domain.HasDimension(attributeOrDimension)
			if err != nil {
				return nil, err
			}

			if hasDim {
				dimension, err := domain.DimensionFromName(attributeOrDimension)
				if err != nil {
					return nil, fmt.Errorf("Could not get attribute or dimension for SetBuffer: %s", attributeOrDimension)
				}

				datatype, err = dimension.Type()
				if err != nil {
					return nil, fmt.Errorf("Could not get dimensionType for SetBuffer: %s", attributeOrDimension)
				}
			} else {
				// Get the attribute
				attribute, err := schema.AttributeFromName(attributeOrDimension)
				if err != nil {
					return nil, fmt.Errorf("Could not get attribute %s for ResultBufferElements: %s", attributeOrDimension, err)
				}

				// Get datatype size to convert byte lengths to needed buffer sizes
				datatype, err = attribute.Type()
				if err != nil {
					return nil, fmt.Errorf("Could not get attribute type for ResultBufferElements: %s", err)
				}
			}

			// Number of buffer elements is calculated
			bufferElements := (*v[1]) / datatype.Size()
			elements[attributeOrDimension] = [3]uint64{offsetElements, bufferElements, validityElements}
		}
	}

	return elements, nil
}

// BufferVar returns a slice backed by the underlying c buffer from tiledb for
// offets and values
func (q *Query) BufferVar(attributeOrDimension string) ([]uint64, interface{}, error) {
	var datatype Datatype
	schema, err := q.array.Schema()
	if err != nil {
		return nil, nil, err
	}

	domain, err := schema.Domain()
	if err != nil {
		return nil, nil, fmt.Errorf(
			"Could not get domain from array schema for BufferVar: %s",
			err)
	}

	if attributeOrDimension == TILEDB_COORDS {
		datatype, err = domain.Type()
		if err != nil {
			return nil, nil, err
		}
	} else {
		hasDim, err := domain.HasDimension(attributeOrDimension)
		if err != nil {
			return nil, nil, err
		}

		if hasDim {
			dimension, err := domain.DimensionFromName(attributeOrDimension)
			if err != nil {
				return nil, nil, fmt.Errorf("Could not get attribute or dimension for BufferVar: %s", attributeOrDimension)
			}

			datatype, err = dimension.Type()
			if err != nil {
				return nil, nil, fmt.Errorf("Could not get dimensionType for BufferVar: %s", attributeOrDimension)
			}
		} else {
			attribute, err := schema.AttributeFromName(attributeOrDimension)
			if err != nil {
				return nil, nil, fmt.Errorf("Could not get attribute for BufferVar: %s", attributeOrDimension)
			}

			datatype, err = attribute.Type()
			if err != nil {
				return nil, nil, fmt.Errorf("Could not get attributeType for BufferVar: %s", attributeOrDimension)
			}
		}
	}

	cattributeNameOrDimension := C.CString(attributeOrDimension)
	defer C.free(unsafe.Pointer(cattributeNameOrDimension))

	var ret C.int32_t
	var cbufferSize *C.uint64_t
	var cbuffer unsafe.Pointer
	var buffer interface{}
	var coffsetsSize *C.uint64_t
	var coffsets *C.uint64_t
	var offsets []uint64
	switch datatype {
	case TILEDB_INT8:
		ret = C.tiledb_query_get_buffer_var(q.context.tiledbContext, q.tiledbQuery, cattributeNameOrDimension, &coffsets, &coffsetsSize, &cbuffer, &cbufferSize)
		length := (*cbufferSize) / C.sizeof_int8_t
		buffer = (*[1 << 46]int8)(cbuffer)[:length:length]
		offsetsLength := *coffsetsSize / C.sizeof_uint64_t
		offsets = (*[1 << 46]uint64)(unsafe.Pointer(coffsets))[:offsetsLength:offsetsLength]

	case TILEDB_INT16:
		ret = C.tiledb_query_get_buffer_var(q.context.tiledbContext, q.tiledbQuery, cattributeNameOrDimension, &coffsets, &coffsetsSize, &cbuffer, &cbufferSize)
		length := (*cbufferSize) / C.sizeof_int16_t
		buffer = (*[1 << 46]int16)(cbuffer)[:length:length]
		offsetsLength := *coffsetsSize / C.sizeof_uint64_t
		offsets = (*[1 << 46]uint64)(unsafe.Pointer(coffsets))[:offsetsLength:offsetsLength]

	case TILEDB_INT32:
		ret = C.tiledb_query_get_buffer_var(q.context.tiledbContext, q.tiledbQuery, cattributeNameOrDimension, &coffsets, &coffsetsSize, &cbuffer, &cbufferSize)
		length := (*cbufferSize) / C.sizeof_int32_t
		buffer = (*[1 << 46]int32)(cbuffer)[:length:length]
		offsetsLength := *coffsetsSize / C.sizeof_uint64_t
		offsets = (*[1 << 46]uint64)(unsafe.Pointer(coffsets))[:offsetsLength:offsetsLength]

	case TILEDB_INT64, TILEDB_DATETIME_YEAR, TILEDB_DATETIME_MONTH, TILEDB_DATETIME_WEEK, TILEDB_DATETIME_DAY, TILEDB_DATETIME_HR, TILEDB_DATETIME_MIN, TILEDB_DATETIME_SEC, TILEDB_DATETIME_MS, TILEDB_DATETIME_US, TILEDB_DATETIME_NS, TILEDB_DATETIME_PS, TILEDB_DATETIME_FS, TILEDB_DATETIME_AS, TILEDB_TIME_HR, TILEDB_TIME_MIN, TILEDB_TIME_SEC, TILEDB_TIME_MS, TILEDB_TIME_US, TILEDB_TIME_NS, TILEDB_TIME_PS, TILEDB_TIME_FS, TILEDB_TIME_AS:
		ret = C.tiledb_query_get_buffer_var(q.context.tiledbContext, q.tiledbQuery, cattributeNameOrDimension, &coffsets, &coffsetsSize, &cbuffer, &cbufferSize)
		length := (*cbufferSize) / C.sizeof_int64_t
		buffer = (*[1 << 46]int64)(cbuffer)[:length:length]
		offsetsLength := *coffsetsSize / C.sizeof_uint64_t
		offsets = (*[1 << 46]uint64)(unsafe.Pointer(coffsets))[:offsetsLength:offsetsLength]

	case TILEDB_UINT8:
		ret = C.tiledb_query_get_buffer_var(q.context.tiledbContext, q.tiledbQuery, cattributeNameOrDimension, &coffsets, &coffsetsSize, &cbuffer, &cbufferSize)
		length := (*cbufferSize) / C.sizeof_uint8_t
		buffer = (*[1 << 46]uint8)(cbuffer)[:length:length]
		offsetsLength := *coffsetsSize / C.sizeof_uint64_t
		offsets = (*[1 << 46]uint64)(unsafe.Pointer(coffsets))[:offsetsLength:offsetsLength]

	case TILEDB_UINT16:
		ret = C.tiledb_query_get_buffer_var(q.context.tiledbContext, q.tiledbQuery, cattributeNameOrDimension, &coffsets, &coffsetsSize, &cbuffer, &cbufferSize)
		length := (*cbufferSize) / C.sizeof_uint16_t
		buffer = (*[1 << 46]uint16)(cbuffer)[:length:length]
		offsetsLength := *coffsetsSize / C.sizeof_uint64_t
		offsets = (*[1 << 46]uint64)(unsafe.Pointer(coffsets))[:offsetsLength:offsetsLength]

	case TILEDB_UINT32:
		ret = C.tiledb_query_get_buffer_var(q.context.tiledbContext, q.tiledbQuery, cattributeNameOrDimension, &coffsets, &coffsetsSize, &cbuffer, &cbufferSize)
		length := (*cbufferSize) / C.sizeof_uint32_t
		buffer = (*[1 << 46]uint32)(cbuffer)[:length:length]
		offsetsLength := *coffsetsSize / C.sizeof_uint64_t
		offsets = (*[1 << 46]uint64)(unsafe.Pointer(coffsets))[:offsetsLength:offsetsLength]

	case TILEDB_UINT64:
		ret = C.tiledb_query_get_buffer_var(q.context.tiledbContext, q.tiledbQuery, cattributeNameOrDimension, &coffsets, &coffsetsSize, &cbuffer, &cbufferSize)
		length := (*cbufferSize) / C.sizeof_uint64_t
		buffer = (*[1 << 46]uint64)(cbuffer)[:length:length]
		offsetsLength := *coffsetsSize / C.sizeof_uint64_t
		offsets = (*[1 << 46]uint64)(unsafe.Pointer(coffsets))[:offsetsLength:offsetsLength]

	case TILEDB_FLOAT32:
		ret = C.tiledb_query_get_buffer_var(q.context.tiledbContext, q.tiledbQuery, cattributeNameOrDimension, &coffsets, &coffsetsSize, &cbuffer, &cbufferSize)
		length := (*cbufferSize) / C.sizeof_float
		buffer = (*[1 << 46]float32)(cbuffer)[:length:length]
		offsetsLength := *coffsetsSize / C.sizeof_uint64_t
		offsets = (*[1 << 46]uint64)(unsafe.Pointer(coffsets))[:offsetsLength:offsetsLength]

	case TILEDB_FLOAT64:
		ret = C.tiledb_query_get_buffer_var(q.context.tiledbContext, q.tiledbQuery, cattributeNameOrDimension, &coffsets, &coffsetsSize, &cbuffer, &cbufferSize)
		length := (*cbufferSize) / C.sizeof_double
		buffer = (*[1 << 46]float64)(cbuffer)[:length:length]
		offsetsLength := *coffsetsSize / C.sizeof_uint64_t
		offsets = (*[1 << 46]uint64)(unsafe.Pointer(coffsets))[:offsetsLength:offsetsLength]

	case TILEDB_CHAR:
		ret = C.tiledb_query_get_buffer_var(q.context.tiledbContext, q.tiledbQuery, cattributeNameOrDimension, &coffsets, &coffsetsSize, &cbuffer, &cbufferSize)
		length := (*cbufferSize) / C.sizeof_char
		buffer = (*[1 << 46]byte)(cbuffer)[:length:length]
		offsetsLength := *coffsetsSize / C.sizeof_uint64_t
		offsets = (*[1 << 46]uint64)(unsafe.Pointer(coffsets))[:offsetsLength:offsetsLength]

	case TILEDB_STRING_ASCII:
		ret = C.tiledb_query_get_buffer_var(q.context.tiledbContext, q.tiledbQuery, cattributeNameOrDimension, &coffsets, &coffsetsSize, &cbuffer, &cbufferSize)
		length := (*cbufferSize) / C.sizeof_uint8_t
		buffer = (*[1 << 46]uint8)(cbuffer)[:length:length]
		offsetsLength := *coffsetsSize / C.sizeof_uint64_t
		offsets = (*[1 << 46]uint64)(unsafe.Pointer(coffsets))[:offsetsLength:offsetsLength]

	case TILEDB_STRING_UTF8:
		ret = C.tiledb_query_get_buffer_var(q.context.tiledbContext, q.tiledbQuery, cattributeNameOrDimension, &coffsets, &coffsetsSize, &cbuffer, &cbufferSize)
		length := (*cbufferSize) / C.sizeof_uint8_t
		buffer = (*[1 << 46]uint8)(cbuffer)[:length:length]
		offsetsLength := *coffsetsSize / C.sizeof_uint64_t
		offsets = (*[1 << 46]uint64)(unsafe.Pointer(coffsets))[:offsetsLength:offsetsLength]

	case TILEDB_STRING_UTF16:
		ret = C.tiledb_query_get_buffer_var(q.context.tiledbContext, q.tiledbQuery, cattributeNameOrDimension, &coffsets, &coffsetsSize, &cbuffer, &cbufferSize)
		length := (*cbufferSize) / C.sizeof_uint16_t
		buffer = (*[1 << 46]uint16)(cbuffer)[:length:length]
		offsetsLength := *coffsetsSize / C.sizeof_uint64_t
		offsets = (*[1 << 46]uint64)(unsafe.Pointer(coffsets))[:offsetsLength:offsetsLength]

	case TILEDB_STRING_UTF32:
		ret = C.tiledb_query_get_buffer_var(q.context.tiledbContext, q.tiledbQuery, cattributeNameOrDimension, &coffsets, &coffsetsSize, &cbuffer, &cbufferSize)
		length := (*cbufferSize) / C.sizeof_uint32_t
		buffer = (*[1 << 46]uint32)(cbuffer)[:length:length]
		offsetsLength := *coffsetsSize / C.sizeof_uint64_t
		offsets = (*[1 << 46]uint64)(unsafe.Pointer(coffsets))[:offsetsLength:offsetsLength]

	case TILEDB_STRING_UCS2:
		ret = C.tiledb_query_get_buffer_var(q.context.tiledbContext, q.tiledbQuery, cattributeNameOrDimension, &coffsets, &coffsetsSize, &cbuffer, &cbufferSize)
		length := (*cbufferSize) / C.sizeof_uint16_t
		buffer = (*[1 << 46]uint16)(cbuffer)[:length:length]
		offsetsLength := *coffsetsSize / C.sizeof_uint64_t
		offsets = (*[1 << 46]uint64)(unsafe.Pointer(coffsets))[:offsetsLength:offsetsLength]

	case TILEDB_STRING_UCS4:
		ret = C.tiledb_query_get_buffer_var(q.context.tiledbContext, q.tiledbQuery, cattributeNameOrDimension, &coffsets, &coffsetsSize, &cbuffer, &cbufferSize)
		length := (*cbufferSize) / C.sizeof_uint32_t
		buffer = (*[1 << 46]uint32)(cbuffer)[:length:length]
		offsetsLength := *coffsetsSize / C.sizeof_uint64_t
		offsets = (*[1 << 46]uint64)(unsafe.Pointer(coffsets))[:offsetsLength:offsetsLength]

	case TILEDB_ANY:
		ret = C.tiledb_query_get_buffer_var(q.context.tiledbContext, q.tiledbQuery, cattributeNameOrDimension, &coffsets, &coffsetsSize, &cbuffer, &cbufferSize)
		length := (*cbufferSize) / C.sizeof_int32_t
		buffer = (*[1 << 46]C.int8_t)(cbuffer)[:length:length]
		offsetsLength := *coffsetsSize / C.sizeof_uint64_t
		offsets = (*[1 << 46]uint64)(unsafe.Pointer(coffsets))[:offsetsLength:offsetsLength]

	default:
		return nil, nil, fmt.Errorf("Unrecognized attribute type: %d", datatype)
	}
	if ret != C.TILEDB_OK {
		return nil, nil, fmt.Errorf("Error getting tiledb query buffer for %s: %s", attributeOrDimension, q.context.LastError())
	}

	return offsets, buffer, nil
}

// BufferVarNullable returns a slice backed by the underlying c buffer from tiledb for
// offets, validities, and values
func (q *Query) BufferVarNullable(attributeOrDimension string) ([]uint64, interface{}, []uint8, error) {
	schema, err := q.array.Schema()
	if err != nil {
		return nil, nil, nil, err
	}

	domain, err := schema.Domain()
	if err != nil {
		return nil, nil, nil, fmt.Errorf("Could not get domain from array schema for BufferVarNullable: %s", err)
	}

	var datatype Datatype
	if attributeOrDimension == TILEDB_COORDS {
		datatype, err = domain.Type()
		if err != nil {
			return nil, nil, nil, err
		}
	} else {
		hasDim, err := domain.HasDimension(attributeOrDimension)
		if err != nil {
			return nil, nil, nil, err
		}

		if hasDim {
			dimension, err := domain.DimensionFromName(attributeOrDimension)
			if err != nil {
				return nil, nil, nil, fmt.Errorf("Could not get attribute or dimension for BufferVarNullable: %s", attributeOrDimension)
			}

			datatype, err = dimension.Type()
			if err != nil {
				return nil, nil, nil, fmt.Errorf("Could not get dimensionType for BufferVarNullable: %s", attributeOrDimension)
			}
		} else {
			attribute, err := schema.AttributeFromName(attributeOrDimension)
			if err != nil {
				return nil, nil, nil, fmt.Errorf("Could not get attribute for BufferVarNullable: %s", attributeOrDimension)
			}

			datatype, err = attribute.Type()
			if err != nil {
				return nil, nil, nil, fmt.Errorf("Could not get attributeType for BufferVarNullable: %s", attributeOrDimension)
			}
		}
	}

	cattributeNameOrDimension := C.CString(attributeOrDimension)
	defer C.free(unsafe.Pointer(cattributeNameOrDimension))

	var ret C.int32_t
	var cbuffer unsafe.Pointer
	var cbufferSize, coffsetsSize, coffsets, cvalidityByteMapSize *C.uint64_t
	var cvalidityByteMap *C.uint8_t

	ret = C.tiledb_query_get_buffer_var_nullable(q.context.tiledbContext, q.tiledbQuery, cattributeNameOrDimension, &coffsets, &coffsetsSize, &cbuffer, &cbufferSize, &cvalidityByteMap, &cvalidityByteMapSize)
	if ret != C.TILEDB_OK {
		return nil, nil, nil, fmt.Errorf("Error getting tiledb query buffer for %s: %s", attributeOrDimension, q.context.LastError())
	}

	// build offsets
	offsetsLength := *coffsetsSize / C.sizeof_uint64_t
	offsets := (*[1 << 46]uint64)(unsafe.Pointer(coffsets))[:offsetsLength:offsetsLength]

	// build validities
	validityByteMapLength := *cvalidityByteMapSize / C.sizeof_uint8_t
	validities := (*[1 << 46]uint8)(unsafe.Pointer(cvalidityByteMap))[:validityByteMapLength:validityByteMapLength]

	// build buffer
	var buffer interface{}
	switch datatype {
	case TILEDB_INT8:
		length := (*cbufferSize) / C.sizeof_int8_t
		buffer = (*[1 << 46]int8)(cbuffer)[:length:length]

	case TILEDB_INT16:
		length := (*cbufferSize) / C.sizeof_int16_t
		buffer = (*[1 << 46]int16)(cbuffer)[:length:length]

	case TILEDB_INT32:
		length := (*cbufferSize) / C.sizeof_int32_t
		buffer = (*[1 << 46]int32)(cbuffer)[:length:length]

	case TILEDB_INT64, TILEDB_DATETIME_YEAR, TILEDB_DATETIME_MONTH, TILEDB_DATETIME_WEEK, TILEDB_DATETIME_DAY, TILEDB_DATETIME_HR, TILEDB_DATETIME_MIN, TILEDB_DATETIME_SEC, TILEDB_DATETIME_MS, TILEDB_DATETIME_US, TILEDB_DATETIME_NS, TILEDB_DATETIME_PS, TILEDB_DATETIME_FS, TILEDB_DATETIME_AS, TILEDB_TIME_HR, TILEDB_TIME_MIN, TILEDB_TIME_SEC, TILEDB_TIME_MS, TILEDB_TIME_US, TILEDB_TIME_NS, TILEDB_TIME_PS, TILEDB_TIME_FS, TILEDB_TIME_AS:
		length := (*cbufferSize) / C.sizeof_int64_t
		buffer = (*[1 << 46]int64)(cbuffer)[:length:length]

	case TILEDB_UINT8:
		length := (*cbufferSize) / C.sizeof_uint8_t
		buffer = (*[1 << 46]uint8)(cbuffer)[:length:length]

	case TILEDB_UINT16:
		length := (*cbufferSize) / C.sizeof_uint16_t
		buffer = (*[1 << 46]uint16)(cbuffer)[:length:length]

	case TILEDB_UINT32:
		length := (*cbufferSize) / C.sizeof_uint32_t
		buffer = (*[1 << 46]uint32)(cbuffer)[:length:length]

	case TILEDB_UINT64:
		length := (*cbufferSize) / C.sizeof_uint64_t
		buffer = (*[1 << 46]uint64)(cbuffer)[:length:length]

	case TILEDB_FLOAT32:
		length := (*cbufferSize) / C.sizeof_float
		buffer = (*[1 << 46]float32)(cbuffer)[:length:length]

	case TILEDB_FLOAT64:
		length := (*cbufferSize) / C.sizeof_double
		buffer = (*[1 << 46]float64)(cbuffer)[:length:length]

	case TILEDB_CHAR:
		length := (*cbufferSize) / C.sizeof_char
		buffer = (*[1 << 46]byte)(cbuffer)[:length:length]

	case TILEDB_STRING_ASCII:
		length := (*cbufferSize) / C.sizeof_uint8_t
		buffer = (*[1 << 46]uint8)(cbuffer)[:length:length]

	case TILEDB_STRING_UTF8:
		length := (*cbufferSize) / C.sizeof_uint8_t
		buffer = (*[1 << 46]uint8)(cbuffer)[:length:length]

	case TILEDB_STRING_UTF16:
		length := (*cbufferSize) / C.sizeof_uint16_t
		buffer = (*[1 << 46]uint16)(cbuffer)[:length:length]

	case TILEDB_STRING_UTF32:
		length := (*cbufferSize) / C.sizeof_uint32_t
		buffer = (*[1 << 46]uint32)(cbuffer)[:length:length]

	case TILEDB_STRING_UCS2:
		length := (*cbufferSize) / C.sizeof_uint16_t
		buffer = (*[1 << 46]uint16)(cbuffer)[:length:length]

	case TILEDB_STRING_UCS4:
		length := (*cbufferSize) / C.sizeof_uint32_t
		buffer = (*[1 << 46]uint32)(cbuffer)[:length:length]

	case TILEDB_ANY:
		length := (*cbufferSize) / C.sizeof_int32_t
		buffer = (*[1 << 46]C.int8_t)(cbuffer)[:length:length]

	default:
		return nil, nil, nil, fmt.Errorf("Unrecognized attribute type: %d", datatype)
	}

	return offsets, buffer, validities, nil
}

// BufferSizeVar returns the size (in num elements) of the backing C buffers for the given variable-length attribute
func (q *Query) BufferSizeVar(attributeOrDimension string) (uint64, uint64, error) {
	var datatype Datatype
	schema, err := q.array.Schema()
	if err != nil {
		return 0, 0, err
	}

	domain, err := schema.Domain()
	if err != nil {
		return 0, 0, fmt.Errorf(
			"Could not get domain from array schema for BufferSizeVar: %s",
			err)
	}

	if attributeOrDimension == TILEDB_COORDS {
		datatype, err = domain.Type()
		if err != nil {
			return 0, 0, err
		}
	} else {
		hasDim, err := domain.HasDimension(attributeOrDimension)
		if err != nil {
			return 0, 0, err
		}

		if hasDim {
			dimension, err := domain.DimensionFromName(attributeOrDimension)
			if err != nil {
				return 0, 0, fmt.Errorf("Could not get attribute or dimension for BufferSizeVar: %s", attributeOrDimension)
			}

			datatype, err = dimension.Type()
			if err != nil {
				return 0, 0, fmt.Errorf("Could not get dimensionType for BufferSizeVar: %s", attributeOrDimension)
			}
		} else {
			attribute, err := schema.AttributeFromName(attributeOrDimension)
			if err != nil {
				return 0, 0, fmt.Errorf("Could not get attribute %s for BufferSizeVar", attributeOrDimension)
			}

			datatype, err = attribute.Type()
			if err != nil {
				return 0, 0, fmt.Errorf("Could not get attributeType for BufferSizeVar: %s", attributeOrDimension)
			}
		}
	}

	dataTypeSize := datatype.Size()
	offsetTypeSize := TILEDB_UINT64.Size()

	cattributeNameOrDimension := C.CString(attributeOrDimension)
	defer C.free(unsafe.Pointer(cattributeNameOrDimension))

	var ret C.int32_t
	var cbufferSize *C.uint64_t
	var cbuffer unsafe.Pointer
	var coffsetsSize *C.uint64_t
	var coffsets *C.uint64_t
	ret = C.tiledb_query_get_buffer_var(q.context.tiledbContext, q.tiledbQuery, cattributeNameOrDimension, &coffsets, &coffsetsSize, &cbuffer, &cbufferSize)
	if ret != C.TILEDB_OK {
		return 0, 0, fmt.Errorf("Error getting tiledb query buffer for %s: %s", attributeOrDimension, q.context.LastError())
	}

	var offsetNumElements uint64
	if coffsetsSize == nil {
		offsetNumElements = 0
	} else {
		offsetNumElements = uint64(*coffsetsSize) / offsetTypeSize
	}

	var dataNumElements uint64
	if cbufferSize == nil {
		dataNumElements = 0
	} else {
		dataNumElements = uint64(*cbufferSize) / dataTypeSize
	}

	return offsetNumElements, dataNumElements, nil
}

// BufferSizeVarNullable returns the size (in num elements) of the backing C buffers for the given variable-length nullable attribute
func (q *Query) BufferSizeVarNullable(attributeName string) (uint64, uint64, uint64, error) {
	var datatype Datatype
	schema, err := q.array.Schema()
	if err != nil {
		return 0, 0, 0, err
	}

	attribute, err := schema.AttributeFromName(attributeName)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("Could not get attribute %s for BufferSizeVarNullable", attributeName)
	}

	datatype, err = attribute.Type()
	if err != nil {
		return 0, 0, 0, fmt.Errorf("Could not get attributeType for BufferSizeVarNullable: %s", attributeName)
	}

	dataTypeSize := datatype.Size()
	offsetTypeSize := TILEDB_UINT64.Size()
	validityTypeSize := TILEDB_UINT8.Size()

	cattributeNameOrDimension := C.CString(attributeName)
	defer C.free(unsafe.Pointer(cattributeNameOrDimension))

	var ret C.int32_t
	var cbufferSize *C.uint64_t
	var cbuffer unsafe.Pointer
	var coffsetsSize *C.uint64_t
	var coffsets *C.uint64_t
	var validityByteMap *C.uint8_t
	var validityByteMapSize *C.uint64_t
	ret = C.tiledb_query_get_buffer_var_nullable(q.context.tiledbContext, q.tiledbQuery, cattributeNameOrDimension, &coffsets, &coffsetsSize, &cbuffer, &cbufferSize, &validityByteMap, &validityByteMapSize)
	if ret != C.TILEDB_OK {
		return 0, 0, 0, fmt.Errorf("Error getting tiledb query buffer for %s: %s", attributeName, q.context.LastError())
	}

	var offsetNumElements uint64
	if coffsetsSize == nil {
		offsetNumElements = 0
	} else {
		offsetNumElements = uint64(*coffsetsSize) / offsetTypeSize
	}

	var dataNumElements uint64
	if cbufferSize == nil {
		dataNumElements = 0
	} else {
		dataNumElements = uint64(*cbufferSize) / dataTypeSize
	}

	var validityNumElements uint64
	if validityByteMapSize == nil {
		validityNumElements = 0
	} else {
		validityNumElements = uint64(*validityByteMapSize) / validityTypeSize
	}

	return offsetNumElements, dataNumElements, validityNumElements, nil
}

// BufferSize returns the size (in num elements) of the backing C buffer for the given attribute
func (q *Query) BufferSize(attributeNameOrDimension string) (uint64, error) {
	var datatype Datatype
	schema, err := q.array.Schema()
	if err != nil {
		return 0, err
	}

	domain, err := schema.Domain()
	if err != nil {
		return 0, fmt.Errorf(
			"Could not get domain from array schema for BufferSize: %s",
			err)
	}

	if attributeNameOrDimension == TILEDB_COORDS {
		datatype, err = domain.Type()
		if err != nil {
			return 0, err
		}
	} else {
		hasDim, err := domain.HasDimension(attributeNameOrDimension)
		if err != nil {
			return 0, err
		}

		if hasDim {
			dimension, err := domain.DimensionFromName(attributeNameOrDimension)
			if err != nil {
				return 0, fmt.Errorf("Could not get attribute or dimension for BufferSize: %s", attributeNameOrDimension)
			}

			datatype, err = dimension.Type()
			if err != nil {
				return 0, fmt.Errorf("Could not get dimensionType for BufferSize: %s", attributeNameOrDimension)
			}
		} else {
			attribute, err := schema.AttributeFromName(attributeNameOrDimension)
			if err != nil {
				return 0, err
			}

			datatype, err = attribute.Type()
			if err != nil {
				return 0, err
			}
		}
	}

	dataTypeSize := datatype.Size()

	cattributeNameOrDimension := C.CString(attributeNameOrDimension)
	defer C.free(unsafe.Pointer(cattributeNameOrDimension))

	var ret C.int32_t
	var cbufferSize *C.uint64_t
	var cbuffer unsafe.Pointer
	ret = C.tiledb_query_get_buffer(q.context.tiledbContext, q.tiledbQuery, cattributeNameOrDimension, &cbuffer, &cbufferSize)
	if ret != C.TILEDB_OK {
		return 0, fmt.Errorf("Error getting tiledb query buffer for %s: %s", attributeNameOrDimension, q.context.LastError())
	}

	var dataNumElements uint64
	if cbufferSize == nil {
		dataNumElements = 0
	} else {
		dataNumElements = uint64(*cbufferSize) / dataTypeSize
	}

	return dataNumElements, nil
}

// BufferSizeNullable returns the size (in num elements) of the backing C buffer for the given nullable attribute
func (q *Query) BufferSizeNullable(attributeName string) (uint64, uint64, error) {
	var datatype Datatype
	schema, err := q.array.Schema()
	if err != nil {
		return 0, 0, err
	}

	attribute, err := schema.AttributeFromName(attributeName)
	if err != nil {
		return 0, 0, fmt.Errorf("Could not get attribute %s for BufferSizeNullable", attributeName)
	}

	datatype, err = attribute.Type()
	if err != nil {
		return 0, 0, fmt.Errorf("Could not get attributeType for BufferSizeNullable: %s", attributeName)
	}

	dataTypeSize := datatype.Size()
	validityTypeSize := TILEDB_UINT8.Size()

	cattributeNameOrDimension := C.CString(attributeName)
	defer C.free(unsafe.Pointer(cattributeNameOrDimension))

	var ret C.int32_t
	var cbufferSize *C.uint64_t
	var cbuffer unsafe.Pointer
	var validityByteMap *C.uint8_t
	var validityByteMapSize *C.uint64_t
	ret = C.tiledb_query_get_buffer_nullable(q.context.tiledbContext, q.tiledbQuery, cattributeNameOrDimension, &cbuffer, &cbufferSize, &validityByteMap, &validityByteMapSize)
	if ret != C.TILEDB_OK {
		return 0, 0, fmt.Errorf("Error getting tiledb query buffer for %s: %s", attributeName, q.context.LastError())
	}

	var dataNumElements uint64
	if cbufferSize == nil {
		dataNumElements = 0
	} else {
		dataNumElements = uint64(*cbufferSize) / dataTypeSize
	}

	var validityNumElements uint64
	if validityByteMapSize == nil {
		validityNumElements = 0
	} else {
		validityNumElements = uint64(*validityByteMapSize) / validityTypeSize
	}

	return dataNumElements, validityNumElements, nil
}

// SetLayout sets the layout of the cells to be written or read
func (q *Query) SetLayout(layout Layout) error {
	ret := C.tiledb_query_set_layout(q.context.tiledbContext, q.tiledbQuery, C.tiledb_layout_t(layout))
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error setting query layout: %s", q.context.LastError())
	}
	return nil
}

// SetQueryCondition sets a query condition on a read query
func (q *Query) SetQueryCondition(cond *QueryCondition) error {
	if ret := C.tiledb_query_set_condition(q.context.tiledbContext, q.tiledbQuery, cond.cond); ret != C.TILEDB_OK {
		return fmt.Errorf("Error getting config from query: %s", q.context.LastError())
	}
	return nil
}

// Finalize Flushes all internal state of a query object and finalizes the
// query. This is applicable only to global layout writes. It has no effect
// for any other query type.
func (q *Query) Finalize() error {
	ret := C.tiledb_query_finalize(q.context.tiledbContext, q.tiledbQuery)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error finalizing query: %s", q.context.LastError())
	}
	q.bufferMutex.Lock()
	defer q.bufferMutex.Unlock()
	q.buffers = nil
	return nil
}

/*
Submit a TileDB query
This will block until query is completed

Note:
Finalize() must be invoked after finish writing in global layout
(via repeated invocations of Submit()), in order to flush any internal state.
For the case of reads, if the returned status is TILEDB_INCOMPLETE, TileDB
could not fit the entire result in the user’s buffers. In this case, the user
should consume the read results (if any), optionally reset the buffers with
SetBuffer(), and then resubmit the query until the status becomes
TILEDB_COMPLETED. If all buffer sizes after the termination of this
function become 0, then this means that no useful data was read into
the buffers, implying that the larger buffers are needed for the query
to proceed. In this case, the users must reallocate their buffers
(increasing their size), reset the buffers with set_buffer(),
and resubmit the query.

*/
func (q *Query) Submit() error {
	ret := C.tiledb_query_submit(q.context.tiledbContext, q.tiledbQuery)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error submitting query: %s", q.context.LastError())
	}

	return nil
}

/*
SubmitAsync a TileDB query

Async does not currently support the callback function parameter
To monitor progress of a query in a non blocking manner the status can be
polled:

 // Start goroutine for background monitoring
 go func(query Query) {
  var status QueryStatus
  var err error
   for status, err = query.Status(); status == TILEDB_INPROGRESS && err == nil; status, err = query.Status() {
     // Do something while query is running
   }
   // Do something when query is finished
 }(query)
*/
func (q *Query) SubmitAsync() error {
	ret := C.tiledb_query_submit_async(q.context.tiledbContext, q.tiledbQuery, nil, nil)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error submitting query: %s", q.context.LastError())
	}
	return nil
}

// Status returns the status of a query
func (q *Query) Status() (QueryStatus, error) {
	var status C.tiledb_query_status_t
	ret := C.tiledb_query_get_status(q.context.tiledbContext, q.tiledbQuery, &status)
	if ret != C.TILEDB_OK {
		return -1, fmt.Errorf("Error getting query status: %s", q.context.LastError())
	}
	return QueryStatus(status), nil
}

// Type returns the query type
func (q *Query) Type() (QueryType, error) {
	var queryType C.tiledb_query_type_t
	ret := C.tiledb_query_get_type(q.context.tiledbContext, q.tiledbQuery, &queryType)
	if ret != C.TILEDB_OK {
		return -1, fmt.Errorf("Error getting query type: %s", q.context.LastError())
	}
	return QueryType(queryType), nil
}

// HasResults Returns true if the query has results
// Applicable only to read queries (it returns false for write queries)
func (q *Query) HasResults() (bool, error) {
	var hasResults C.int32_t
	ret := C.tiledb_query_has_results(q.context.tiledbContext, q.tiledbQuery, &hasResults)
	if ret != C.TILEDB_OK {
		return false, fmt.Errorf("Error checking if query has results: %s", q.context.LastError())
	}
	return int(hasResults) == 1, nil
}

// EstResultSize gets the query estimated result size in bytes for an attribute
func (q *Query) EstResultSize(attributeName string) (*uint64, error) {
	cAttributeName := C.CString(attributeName)
	defer C.free(unsafe.Pointer(cAttributeName))

	var size uint64

	ret := C.tiledb_query_get_est_result_size(
		q.context.tiledbContext,
		q.tiledbQuery,
		cAttributeName,
		(*C.uint64_t)(unsafe.Pointer(&size)))
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error estimating query result size: %s", q.context.LastError())
	}

	return &size, nil
}

// EstResultSizeVar gets the query estimated result size in bytes for a var sized attribute
func (q *Query) EstResultSizeVar(attributeName string) (*uint64, *uint64, error) {
	cAttributeName := C.CString(attributeName)
	defer C.free(unsafe.Pointer(cAttributeName))

	var sizeOff, sizeVal uint64

	ret := C.tiledb_query_get_est_result_size_var(
		q.context.tiledbContext,
		q.tiledbQuery,
		cAttributeName,
		(*C.uint64_t)(unsafe.Pointer(&sizeOff)),
		(*C.uint64_t)(unsafe.Pointer(&sizeVal)))
	if ret != C.TILEDB_OK {
		return nil, nil, fmt.Errorf("Error estimating query result var size: %s", q.context.LastError())
	}

	return &sizeOff, &sizeVal, nil
}

// EstResultSizeNullable gets the query estimated result size in bytes for an attribute
func (q *Query) EstResultSizeNullable(attributeName string) (*uint64, *uint64, error) {
	cAttributeName := C.CString(attributeName)
	defer C.free(unsafe.Pointer(cAttributeName))

	var size, sizeValidity uint64

	ret := C.tiledb_query_get_est_result_size_nullable(
		q.context.tiledbContext,
		q.tiledbQuery,
		cAttributeName,
		(*C.uint64_t)(unsafe.Pointer(&size)),
		(*C.uint64_t)(unsafe.Pointer(&sizeValidity)))
	if ret != C.TILEDB_OK {
		return nil, nil, fmt.Errorf("Error estimating query result size: %s", q.context.LastError())
	}

	return &size, &sizeValidity, nil
}

// EstResultSizeVarNullable gets the query estimated result size in bytes for a var sized attribute
func (q *Query) EstResultSizeVarNullable(attributeName string) (*uint64, *uint64, *uint64, error) {
	cAttributeName := C.CString(attributeName)
	defer C.free(unsafe.Pointer(cAttributeName))

	var sizeOff, sizeVal, sizeValidity uint64

	ret := C.tiledb_query_get_est_result_size_var_nullable(
		q.context.tiledbContext,
		q.tiledbQuery,
		cAttributeName,
		(*C.uint64_t)(unsafe.Pointer(&sizeOff)),
		(*C.uint64_t)(unsafe.Pointer(&sizeVal)),
		(*C.uint64_t)(unsafe.Pointer(&sizeValidity)))
	if ret != C.TILEDB_OK {
		return nil, nil, nil, fmt.Errorf("Error estimating query result var size: %s", q.context.LastError())
	}

	return &sizeOff, &sizeVal, &sizeValidity, nil
}

/*
EstimateBufferElements compute an upper bound on the buffer elements needed to
read a subarray or range(s)
Returns a map of attribute or dimension name to the maximum
number of elements that can be read in the given subarray. For each attribute,
a pair of numbers are returned. The first, for variable-length attributes, is
the maximum number of offsets for that attribute in the given subarray. For
fixed-length attributes and coordinates, the first is always 0. The second
is the maximum number of elements for that attribute in the given subarray.
*/
func (q *Query) EstimateBufferElements() (map[string][3]uint64, error) {
	// Build map
	ret := make(map[string][3]uint64)
	// Get schema
	schema, err := q.array.Schema()
	if err != nil {
		return nil, fmt.Errorf("Error getting EstimateBufferElements for array: %s", err)
	}

	attributes, err := schema.Attributes()
	if err != nil {
		return nil, fmt.Errorf("Error getting EstimateBufferElements for array: %s", err)
	}
	// Loop through each attribute
	for _, attribute := range attributes {

		// Check if attribute is variable attribute or not
		cellValNum, err := attribute.CellValNum()
		if err != nil {
			return nil, fmt.Errorf("Error getting EstimateBufferElements for array: %s", err)
		}

		// Get datatype size to convert byte lengths to needed buffer sizes
		dataType, err := attribute.Type()
		if err != nil {
			return nil, fmt.Errorf("Error getting EstimateBufferElements for array: %s", err)
		}

		dataTypeSize := dataType.Size()

		// Get attribute name
		name, err := attribute.Name()
		if err != nil {
			return nil, fmt.Errorf("Error getting EstimateBufferElements for array: %s", err)
		}

		nullable, err := attribute.Nullable()
		if err != nil {
			return nil, fmt.Errorf("Error getting EstimateBufferElements for array: %s", err)
		}

		if cellValNum == TILEDB_VAR_NUM {
			if nullable {
				bufferOffsetSize, bufferValSize, bufferValiditySize, err := q.EstResultSizeVarNullable(name)
				if err != nil {
					return nil, fmt.Errorf("Error getting EstimateBufferElements for array: %s", err)
				}
				// Set sizes for attribute in return map
				ret[name] = [3]uint64{
					*bufferOffsetSize / uint64(C.TILEDB_OFFSET_SIZE),
					*bufferValSize / dataTypeSize,
					*bufferValiditySize / bytesizes.Uint8}
			} else {
				bufferOffsetSize, bufferValSize, err := q.EstResultSizeVar(name)
				if err != nil {
					return nil, fmt.Errorf("Error getting EstimateBufferElements for array: %s", err)
				}
				// Set sizes for attribute in return map
				ret[name] = [3]uint64{
					*bufferOffsetSize / uint64(C.TILEDB_OFFSET_SIZE),
					*bufferValSize / dataTypeSize,
					0}
			}
		} else {
			if nullable {
				bufferValSize, bufferValiditySize, err := q.EstResultSizeNullable(name)
				if err != nil {
					return nil, fmt.Errorf("Error getting EstimateBufferElements for array: %s", err)
				}
				ret[name] = [3]uint64{0, *bufferValSize / dataTypeSize,
					*bufferValiditySize / bytesizes.Uint8}
			} else {
				bufferValSize, err := q.EstResultSize(name)
				if err != nil {
					return nil, fmt.Errorf("Error getting EstimateBufferElements for array: %s", err)
				}
				ret[name] = [3]uint64{0, *bufferValSize / dataTypeSize, 0}
			}
		}
	}

	// Handle coordinates
	domain, err := schema.Domain()
	if err != nil {
		return nil, fmt.Errorf("Could not get domain for EstimateBufferElements: %s", err)
	}

	ndims, err := domain.NDim()
	if err != nil {
		return nil, err
	}

	for dimIdx := uint(0); dimIdx < ndims; dimIdx++ {
		dim, err := domain.DimensionFromIndex(dimIdx)
		if err != nil {
			return nil, err
		}

		dimType, err := dim.Type()
		if err != nil {
			return nil, err
		}

		dataTypeSize := dimType.Size()

		cellValNum, err := dim.CellValNum()
		if err != nil {
			return nil, fmt.Errorf("Error getting MaxBufferElements for array: %s", err)
		}

		// Get dimension name
		name, err := dim.Name()
		if err != nil {
			return nil, fmt.Errorf("Error getting MaxBufferElements for array: %s", err)
		}

		if cellValNum == TILEDB_VAR_NUM {
			bufferOffsetSize, bufferValSize, err := q.EstResultSizeVar(name)
			if err != nil {
				return nil, fmt.Errorf("Error getting MaxBufferElements for array: %s", err)
			}
			// Set sizes for dimension in return map
			ret[name] = [3]uint64{
				*bufferOffsetSize / uint64(C.TILEDB_OFFSET_SIZE),
				*bufferValSize / dataTypeSize, 0}
		} else {
			bufferValSize, err := q.EstResultSize(name)
			if err != nil {
				return nil, fmt.Errorf("Error getting MaxBufferElements for array: %s", err)
			}
			ret[name] = [3]uint64{0, *bufferValSize / dataTypeSize, 0}
		}
	}

	return ret, nil
}

// GetFragmentNum returns num of fragments
func (q *Query) GetFragmentNum() (*uint32, error) {
	var num uint32

	ret := C.tiledb_query_get_fragment_num(
		q.context.tiledbContext,
		q.tiledbQuery,
		(*C.uint32_t)(unsafe.Pointer(&num)))
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error getting num of fragments: %s", q.context.LastError())
	}

	return &num, nil
}

// GetFragmentURI returns uri for a fragment
func (q *Query) GetFragmentURI(num uint64) (*string, error) {
	var cURI *C.char

	ret := C.tiledb_query_get_fragment_uri(
		q.context.tiledbContext,
		q.tiledbQuery,
		(C.uint64_t)(num),
		&cURI)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error uri for fragment : %d", q.context.LastError())
	}

	uri := C.GoString(cURI)

	return &uri, nil

}

// GetFragmentTimestampRange returns timestamp range for a fragment
func (q *Query) GetFragmentTimestampRange(num uint64) (*uint64, *uint64, error) {
	var t1, t2 uint64

	ret := C.tiledb_query_get_fragment_timestamp_range(
		q.context.tiledbContext,
		q.tiledbQuery,
		(C.uint64_t)(num),
		(*C.uint64_t)(unsafe.Pointer(&t1)),
		(*C.uint64_t)(unsafe.Pointer(&t2)))
	if ret != C.TILEDB_OK {
		return nil, nil, fmt.Errorf("Error getting fragment timestamp: %s", q.context.LastError())
	}

	return &t1, &t2, nil
}

// Array returns array used by query
func (q *Query) Array() (*Array, error) {
	array := Array{context: q.context}
	ret := C.tiledb_query_get_array(q.context.tiledbContext, q.tiledbQuery, &array.tiledbArray)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error getting array from query: %s", q.context.LastError())
	}

	runtime.SetFinalizer(&array, func(array *Array) {
		array.Free()
	})
	return &array, nil
}

// SetConfig config on query
func (q *Query) SetConfig(config *Config) error {
	q.config = config

	ret := C.tiledb_query_set_config(q.context.tiledbContext, q.tiledbQuery, q.config.tiledbConfig)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error setting config on query: %s", q.context.LastError())
	}

	return nil
}

// Config get config on query
func (q *Query) Config() (*Config, error) {
	config := Config{}
	ret := C.tiledb_query_get_config(q.context.tiledbContext, q.tiledbQuery, &config.tiledbConfig)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error getting config from query: %s", q.context.LastError())
	}

	runtime.SetFinalizer(&config, func(config *Config) {
		config.Free()
	})

	if q.config == nil {
		q.config = &config
	}

	return &config, nil
}

// Stats gets query stats for a query as a string
func (q *Query) Stats() (string, error) {
	var stats *C.char
	if ret := C.tiledb_query_get_stats(q.context.tiledbContext, q.tiledbQuery, &stats); ret != C.TILEDB_OK {
		return "", fmt.Errorf("Error getting stats from query: %s", q.context.LastError())
	}

	s := C.GoString(stats)
	if ret := C.tiledb_stats_free_str(&stats); ret != C.TILEDB_OK {
		return "", fmt.Errorf("Error freeing string from dumping stats to string")
	}

	return s, nil
}
