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
	"reflect"
	"runtime"
	"sync"
	"unsafe"
)

// Query construct and execute read/write queries on a tiledb Array
type Query struct {
	tiledbQuery          *C.tiledb_query_t
	array                *Array
	context              *Context
	uri                  string
	buffers              []interface{}
	bufferMutex          sync.Mutex
	resultBufferElements map[string][2]*uint64
}

// MarshalJSON marshal arraySchema struct to json using tiledb
func (q *Query) MarshalJSON() ([]byte, error) {
	var jsonString *C.char
	defer C.free(unsafe.Pointer(jsonString))
	var jsonStringLength C.uint64_t
	ret := C.tiledb_query_serialize(q.context.tiledbContext, q.tiledbQuery, C.tiledb_serialization_type_t(TILEDB_JSON), &jsonString, &jsonStringLength)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error marshaling json for array schema: %s", q.context.LastError())
	}
	return []byte(C.GoString(jsonString)), nil
}

// UnmarshalJSON marshal arraySchema struct to json using tiledb
func (q *Query) UnmarshalJSON(b []byte) error {
	var err error
	if q.context == nil {
		q.context, err = NewContext(nil)
		if err != nil {
			return err
		}
	}
	if q.array == nil {
		q.array, err = NewArray(q.context, "")
		if err != nil {
			return err
		}
	}
	jsonString := C.CString(string(b))
	defer C.free(unsafe.Pointer(jsonString))
	var jsonStringLength = C.uint64_t(len(b))
	ret := C.tiledb_query_deserialize(q.context.tiledbContext, q.tiledbQuery, C.tiledb_serialization_type_t(TILEDB_JSON), jsonString, jsonStringLength)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error unmarshaling json for array schema: %s", q.context.LastError())
	}
	return nil
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
func NewQuery(ctx *Context, array *Array) (*Query, error) {
	if array == nil {
		return nil, fmt.Errorf("Error creating tiledb query: passed array is nil")
	}

	queryType, err := array.QueryType()
	if err != nil {
		return nil, fmt.Errorf("Error getting QueryType from passed array %s", err)
	}

	query := Query{context: ctx, array: array}
	ret := C.tiledb_query_alloc(query.context.tiledbContext, array.tiledbArray, C.tiledb_query_type_t(queryType), &query.tiledbQuery)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error creating tiledb query: %s", query.context.LastError())
	}

	// Set finalizer for free C pointer on gc
	runtime.SetFinalizer(&query, func(query *Query) {
		query.Free()
	})

	query.resultBufferElements = make(map[string][2]*uint64, 0)

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

// SetBuffer Sets the buffer for a fixed-sized attribute to a query
// The buffer must be an initialized slice
func (q *Query) SetBuffer(attribute string, buffer interface{}) (*uint64,
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

	var attributeType Datatype
	// If we are setting tiledb coordinates for a sparse array we want to check
	// the domain type. The TILEDB_COORDS attribute is only materialized after
	// the first write
	if attribute == TILEDB_COORDS {
		domain, err := schema.Domain()
		if err != nil {
			return nil, fmt.Errorf(
				"Could not get domain for SetBuffer: %s",
				attribute)
		}
		attributeType, err = domain.Type()
		if err != nil {
			return nil, fmt.Errorf(
				"Could not get domainType for SetBuffer: %s",
				attribute)
		}
	} else {
		schemaAttribute, err := schema.AttributeFromName(attribute)
		if err != nil {
			return nil, fmt.Errorf(
				"Could not get attribute for SetBuffer: %s",
				attribute)
		}

		attributeType, err = schemaAttribute.Type()
		if err != nil {
			return nil, fmt.Errorf(
				"Could not get attributeType for SetBuffer: %s",
				attribute)
		}
	}

	bufferType := bufferReflectType.Elem().Kind()
	if attributeType.ReflectKind() != bufferType {
		return nil, fmt.Errorf("Buffer and Attribute do not have the same"+
			" data types. Buffer: %s, Attribute: %s",
			bufferType.String(),
			attributeType.ReflectKind().String())
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
		bufferSize = bufferSize * uint64(unsafe.Sizeof(int(0)))
		// Create buffer void*
		tmpBuffer := buffer.([]int)
		// Store slice so underlying array is not gc'ed
		q.buffers = append(q.buffers, tmpBuffer)
		cbuffer = unsafe.Pointer(&(tmpBuffer)[0])
	case reflect.Int8:
		// Set buffersize
		bufferSize = bufferSize * uint64(unsafe.Sizeof(int8(0)))
		// Create buffer void*
		tmpBuffer := buffer.([]int8)
		// Store slice so underlying array is not gc'ed
		q.buffers = append(q.buffers, tmpBuffer)
		cbuffer = unsafe.Pointer(&(tmpBuffer)[0])
	case reflect.Int16:
		// Set buffersize
		bufferSize = bufferSize * uint64(unsafe.Sizeof(int16(0)))
		// Create buffer void*
		tmpBuffer := buffer.([]int16)
		// Store slice so underlying array is not gc'ed
		q.buffers = append(q.buffers, tmpBuffer)
		cbuffer = unsafe.Pointer(&(tmpBuffer)[0])
	case reflect.Int32:
		// Set buffersize
		bufferSize = bufferSize * uint64(unsafe.Sizeof(int32(0)))
		// Create buffer void*
		tmpBuffer := buffer.([]int32)
		// Store slice so underlying array is not gc'ed
		q.buffers = append(q.buffers, tmpBuffer)
		cbuffer = unsafe.Pointer(&(tmpBuffer)[0])
	case reflect.Int64:
		// Set buffersize
		bufferSize = bufferSize * uint64(unsafe.Sizeof(int64(0)))
		// Create buffer void*
		tmpBuffer := buffer.([]int64)
		// Store slice so underlying array is not gc'ed
		q.buffers = append(q.buffers, tmpBuffer)
		cbuffer = unsafe.Pointer(&(tmpBuffer)[0])
	case reflect.Uint:
		// Set buffersize
		bufferSize = bufferSize * uint64(unsafe.Sizeof(uint(0)))
		// Create buffer void*
		tmpBuffer := buffer.([]uint)
		// Store slice so underlying array is not gc'ed
		q.buffers = append(q.buffers, tmpBuffer)
		cbuffer = unsafe.Pointer(&(tmpBuffer)[0])
	case reflect.Uint8:
		// Set buffersize
		bufferSize = bufferSize * uint64(unsafe.Sizeof(uint8(0)))
		// Create buffer void*
		tmpBuffer := buffer.([]uint8)
		// Store slice so underlying array is not gc'ed
		q.buffers = append(q.buffers, tmpBuffer)
		cbuffer = unsafe.Pointer(&(tmpBuffer)[0])
	case reflect.Uint16:
		// Set buffersize
		bufferSize = bufferSize * uint64(unsafe.Sizeof(uint16(0)))
		// Create buffer void*
		tmpBuffer := buffer.([]uint16)
		// Store slice so underlying array is not gc'ed
		q.buffers = append(q.buffers, tmpBuffer)
		cbuffer = unsafe.Pointer(&(tmpBuffer)[0])
	case reflect.Uint32:
		// Set buffersize
		bufferSize = bufferSize * uint64(unsafe.Sizeof(uint32(0)))
		// Create buffer void*
		tmpBuffer := buffer.([]uint32)
		// Store slice so underlying array is not gc'ed
		q.buffers = append(q.buffers, tmpBuffer)
		cbuffer = unsafe.Pointer(&(tmpBuffer)[0])
	case reflect.Uint64:
		// Set buffersize
		bufferSize = bufferSize * uint64(unsafe.Sizeof(uint64(0)))
		// Create buffer void*
		tmpBuffer := buffer.([]uint64)
		// Store slice so underlying array is not gc'ed
		q.buffers = append(q.buffers, tmpBuffer)
		cbuffer = unsafe.Pointer(&(tmpBuffer)[0])
	case reflect.Float32:
		// Set buffersize
		bufferSize = bufferSize * uint64(unsafe.Sizeof(float32(0)))
		// Create buffer void*
		tmpBuffer := buffer.([]float32)
		// Store slice so underlying array is not gc'ed
		q.buffers = append(q.buffers, tmpBuffer)
		cbuffer = unsafe.Pointer(&(tmpBuffer)[0])
	case reflect.Float64:
		// Set buffersize
		bufferSize = bufferSize * uint64(unsafe.Sizeof(float64(0)))
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

	cAttribute := C.CString(attribute)
	defer C.free(unsafe.Pointer(cAttribute))

	ret := C.tiledb_query_set_buffer(
		q.context.tiledbContext,
		q.tiledbQuery,
		cAttribute,
		cbuffer,
		(*C.uint64_t)(unsafe.Pointer(&bufferSize)))

	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf(
			"Error setting query buffer: %s", q.context.LastError())
	}

	q.resultBufferElements[attribute] =
		[2]*uint64{nil, &bufferSize}

	return &bufferSize, nil
}

// Buffer returns a slice backed by the underlying c buffer from tiledb
func (q *Query) Buffer(attributeName string) (interface{}, error) {
	var datatype Datatype
	schema, err := q.array.Schema()
	if err != nil {
		return nil, err
	}

	if attributeName == TILEDB_COORDS {
		domain, err := schema.Domain()
		if err != nil {
			return nil, err
		}
		datatype, err = domain.Type()
		if err != nil {
			return nil, err
		}
	} else {
		attribute, err := schema.AttributeFromName(attributeName)
		if err != nil {
			return nil, err
		}
		datatype, err = attribute.Type()
		if err != nil {
			return nil, err
		}
	}

	cattributeName := C.CString(attributeName)
	defer C.free(unsafe.Pointer(cattributeName))

	var ret C.int32_t
	var cbufferSize *C.uint64_t
	var cbuffer unsafe.Pointer
	var buffer interface{}
	switch datatype {
	case TILEDB_INT8:
		ret = C.tiledb_query_get_buffer(q.context.tiledbContext, q.tiledbQuery, cattributeName, &cbuffer, &cbufferSize)
		length := (*cbufferSize) / C.sizeof_int8_t
		buffer = (*[1 << 30]int8)(cbuffer)[:length:length]

	case TILEDB_INT16:
		ret = C.tiledb_query_get_buffer(q.context.tiledbContext, q.tiledbQuery, cattributeName, &cbuffer, &cbufferSize)
		length := (*cbufferSize) / C.sizeof_int16_t
		buffer = (*[1 << 30]int16)(cbuffer)[:length:length]

	case TILEDB_INT32:
		ret = C.tiledb_query_get_buffer(q.context.tiledbContext, q.tiledbQuery, cattributeName, &cbuffer, &cbufferSize)
		length := (*cbufferSize) / C.sizeof_int32_t
		buffer = (*[1 << 30]int32)(cbuffer)[:length:length]

	case TILEDB_INT64:
		ret = C.tiledb_query_get_buffer(q.context.tiledbContext, q.tiledbQuery, cattributeName, &cbuffer, &cbufferSize)
		length := (*cbufferSize) / C.sizeof_int64_t
		buffer = (*[1 << 30]int64)(cbuffer)[:length:length]

	case TILEDB_UINT8:
		ret = C.tiledb_query_get_buffer(q.context.tiledbContext, q.tiledbQuery, cattributeName, &cbuffer, &cbufferSize)
		length := (*cbufferSize) / C.sizeof_uint8_t
		buffer = (*[1 << 30]uint8)(cbuffer)[:length:length]

	case TILEDB_UINT16:
		ret = C.tiledb_query_get_buffer(q.context.tiledbContext, q.tiledbQuery, cattributeName, &cbuffer, &cbufferSize)
		length := (*cbufferSize) / C.sizeof_uint16_t
		buffer = (*[1 << 30]uint16)(cbuffer)[:length:length]

	case TILEDB_UINT32:
		ret = C.tiledb_query_get_buffer(q.context.tiledbContext, q.tiledbQuery, cattributeName, &cbuffer, &cbufferSize)
		length := (*cbufferSize) / C.sizeof_uint32_t
		buffer = (*[1 << 30]uint32)(cbuffer)[:length:length]

	case TILEDB_UINT64:
		ret = C.tiledb_query_get_buffer(q.context.tiledbContext, q.tiledbQuery, cattributeName, &cbuffer, &cbufferSize)
		length := (*cbufferSize) / C.sizeof_uint64_t
		buffer = (*[1 << 30]uint64)(cbuffer)[:length:length]

	case TILEDB_FLOAT32:
		ret = C.tiledb_query_get_buffer(q.context.tiledbContext, q.tiledbQuery, cattributeName, &cbuffer, &cbufferSize)
		length := (*cbufferSize) / C.sizeof_float
		buffer = (*[1 << 30]float32)(cbuffer)[:length:length]

	case TILEDB_FLOAT64:
		ret = C.tiledb_query_get_buffer(q.context.tiledbContext, q.tiledbQuery, cattributeName, &cbuffer, &cbufferSize)
		length := (*cbufferSize) / C.sizeof_double
		buffer = (*[1 << 30]float64)(cbuffer)[:length:length]

	case TILEDB_CHAR:
		ret = C.tiledb_query_get_buffer(q.context.tiledbContext, q.tiledbQuery, cattributeName, &cbuffer, &cbufferSize)
		length := (*cbufferSize) / C.sizeof_char
		buffer = (*[1 << 30]byte)(cbuffer)[:length:length]

	case TILEDB_STRING_ASCII:
		ret = C.tiledb_query_get_buffer(q.context.tiledbContext, q.tiledbQuery, cattributeName, &cbuffer, &cbufferSize)
		length := (*cbufferSize) / C.sizeof_uint8_t
		buffer = (*[1 << 30]uint8)(cbuffer)[:length:length]

	case TILEDB_STRING_UTF8:
		ret = C.tiledb_query_get_buffer(q.context.tiledbContext, q.tiledbQuery, cattributeName, &cbuffer, &cbufferSize)
		length := (*cbufferSize) / C.sizeof_uint8_t
		buffer = (*[1 << 30]uint8)(cbuffer)[:length:length]

	case TILEDB_STRING_UTF16:
		ret = C.tiledb_query_get_buffer(q.context.tiledbContext, q.tiledbQuery, cattributeName, &cbuffer, &cbufferSize)
		length := (*cbufferSize) / C.sizeof_uint16_t
		buffer = (*[1 << 30]uint16)(cbuffer)[:length:length]

	case TILEDB_STRING_UTF32:
		ret = C.tiledb_query_get_buffer(q.context.tiledbContext, q.tiledbQuery, cattributeName, &cbuffer, &cbufferSize)
		length := (*cbufferSize) / C.sizeof_uint32_t
		buffer = (*[1 << 30]uint32)(cbuffer)[:length:length]

	case TILEDB_STRING_UCS2:
		ret = C.tiledb_query_get_buffer(q.context.tiledbContext, q.tiledbQuery, cattributeName, &cbuffer, &cbufferSize)
		length := (*cbufferSize) / C.sizeof_uint16_t
		buffer = (*[1 << 30]uint16)(cbuffer)[:length:length]

	case TILEDB_STRING_UCS4:
		ret = C.tiledb_query_get_buffer(q.context.tiledbContext, q.tiledbQuery, cattributeName, &cbuffer, &cbufferSize)
		length := (*cbufferSize) / C.sizeof_uint32_t
		buffer = (*[1 << 30]uint32)(cbuffer)[:length:length]

	case TILEDB_ANY:
		ret = C.tiledb_query_get_buffer(q.context.tiledbContext, q.tiledbQuery, cattributeName, &cbuffer, &cbufferSize)
		length := (*cbufferSize) / C.sizeof_int32_t
		buffer = (*[1 << 30]C.int8_t)(cbuffer)[:length:length]

	default:
		return nil, fmt.Errorf("Unrecognized attribute type: %d", datatype)
	}
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error getting tiledb query buffer for %s: %s", attributeName, q.context.LastError())
	}

	return buffer, nil
}

// SetBufferVar Sets the buffer for a fixed-sized attribute to a query
// The buffer must be an initialized slice
func (q *Query) SetBufferVar(attribute string, offset []uint64,
	buffer interface{}) (*uint64, *uint64, error) {
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

	schemaAttribute, err := schema.AttributeFromName(attribute)
	if err != nil {
		return nil, nil, fmt.Errorf("Could not get attribute for SetBuffer: %s",
			attribute)
	}

	attributeType, err := schemaAttribute.Type()
	if err != nil {
		return nil, nil, fmt.Errorf("Could not get attributeType for SetBuffer: %s",
			attribute)
	}

	bufferType := bufferReflectType.Elem().Kind()
	if attributeType.ReflectKind() != bufferType {
		return nil, nil, fmt.Errorf("Buffer and Attribute do not have the same"+
			" data types. Buffer: %s, Attribute: %s", bufferType.String(), attributeType.ReflectKind().String())
	}

	bufferSize := uint64(bufferReflectValue.Len())

	if bufferSize == uint64(0) {
		return nil, nil, fmt.Errorf("Buffer has no length, " +
			"buffers are required to be initialized before reading or writting")
	}

	offsetSize := uint64(len(offset)) * uint64(unsafe.Sizeof(uint64(0)))

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
		bufferSize = bufferSize * uint64(unsafe.Sizeof(int(0)))

		// Create buffer void*
		tmpBuffer := buffer.([]int)
		// Store slice so underlying array is not gc'ed
		q.buffers = append(q.buffers, tmpBuffer)
		cbuffer = unsafe.Pointer(&(tmpBuffer)[0])
	case reflect.Int8:
		// Set buffersize
		bufferSize = bufferSize * uint64(unsafe.Sizeof(int8(0)))

		// Create buffer void*
		tmpBuffer := buffer.([]int8)
		// Store slice so underlying array is not gc'ed
		q.buffers = append(q.buffers, tmpBuffer)
		cbuffer = unsafe.Pointer(&(tmpBuffer)[0])
	case reflect.Int16:
		// Set buffersize
		bufferSize = bufferSize * uint64(unsafe.Sizeof(int16(0)))

		// Create buffer void*
		tmpBuffer := buffer.([]int16)
		// Store slice so underlying array is not gc'ed
		q.buffers = append(q.buffers, tmpBuffer)
		cbuffer = unsafe.Pointer(&(tmpBuffer)[0])
	case reflect.Int32:
		// Set buffersize
		bufferSize = bufferSize * uint64(unsafe.Sizeof(int32(0)))

		// Create buffer void*
		tmpBuffer := buffer.([]int32)
		// Store slice so underlying array is not gc'ed
		q.buffers = append(q.buffers, tmpBuffer)
		cbuffer = unsafe.Pointer(&(tmpBuffer)[0])
	case reflect.Int64:
		// Set buffersize
		bufferSize = bufferSize * uint64(unsafe.Sizeof(int64(0)))

		// Create buffer void*
		tmpBuffer := buffer.([]int64)
		// Store slice so underlying array is not gc'ed
		q.buffers = append(q.buffers, tmpBuffer)
		cbuffer = unsafe.Pointer(&(tmpBuffer)[0])
	case reflect.Uint:
		// Set buffersize
		bufferSize = bufferSize * uint64(unsafe.Sizeof(uint(0)))

		// Create buffer void*
		tmpBuffer := buffer.([]uint)
		// Store slice so underlying array is not gc'ed
		q.buffers = append(q.buffers, tmpBuffer)
		cbuffer = unsafe.Pointer(&(tmpBuffer)[0])
	case reflect.Uint8:
		// Set buffersize
		bufferSize = bufferSize * uint64(unsafe.Sizeof(uint8(0)))

		// Create buffer void*
		tmpBuffer := buffer.([]uint8)
		// Store slice so underlying array is not gc'ed
		q.buffers = append(q.buffers, tmpBuffer)
		cbuffer = unsafe.Pointer(&(tmpBuffer)[0])
	case reflect.Uint16:
		// Set buffersize
		bufferSize = bufferSize * uint64(unsafe.Sizeof(uint16(0)))

		// Create buffer void*
		tmpBuffer := buffer.([]uint16)
		// Store slice so underlying array is not gc'ed
		q.buffers = append(q.buffers, tmpBuffer)
		cbuffer = unsafe.Pointer(&(tmpBuffer)[0])
	case reflect.Uint32:
		// Set buffersize
		bufferSize = bufferSize * uint64(unsafe.Sizeof(uint32(0)))

		// Create buffer void*
		tmpBuffer := buffer.([]uint32)
		// Store slice so underlying array is not gc'ed
		q.buffers = append(q.buffers, tmpBuffer)
		cbuffer = unsafe.Pointer(&(tmpBuffer)[0])
	case reflect.Uint64:
		// Set buffersize
		bufferSize = bufferSize * uint64(unsafe.Sizeof(uint64(0)))

		// Create buffer void*
		tmpBuffer := buffer.([]uint64)
		// Store slice so underlying array is not gc'ed
		q.buffers = append(q.buffers, tmpBuffer)
		cbuffer = unsafe.Pointer(&(tmpBuffer)[0])
	case reflect.Float32:
		// Set buffersize
		bufferSize = bufferSize * uint64(unsafe.Sizeof(float32(0)))

		// Create buffer void*
		tmpBuffer := buffer.([]float32)
		// Store slice so underlying array is not gc'ed
		q.buffers = append(q.buffers, tmpBuffer)
		cbuffer = unsafe.Pointer(&(tmpBuffer)[0])
	case reflect.Float64:
		// Set buffersize
		bufferSize = bufferSize * uint64(unsafe.Sizeof(float64(0)))

		// Create buffer void*
		tmpBuffer := buffer.([]float64)
		// Store slice so underlying array is not gc'ed
		q.buffers = append(q.buffers, tmpBuffer)
		cbuffer = unsafe.Pointer(&(tmpBuffer)[0])
	default:
		return nil, nil, fmt.Errorf("Unrecognized buffer type passed: %s",
			bufferType.String())
	}

	cAttribute := C.CString(attribute)
	defer C.free(unsafe.Pointer(cAttribute))

	ret := C.tiledb_query_set_buffer_var(
		q.context.tiledbContext,
		q.tiledbQuery,
		cAttribute,
		(*C.uint64_t)(coffset),
		(*C.uint64_t)(unsafe.Pointer(&offsetSize)),
		cbuffer,
		(*C.uint64_t)(unsafe.Pointer(&bufferSize)))

	if ret != C.TILEDB_OK {
		return nil, nil, fmt.Errorf("Error setting query var buffer: %s",
			q.context.LastError())
	}

	q.resultBufferElements[attribute] =
		[2]*uint64{&offsetSize, &bufferSize}

	return &offsetSize, &bufferSize, nil
}

// ResultBufferElements returns the number of elements in the result buffers
// from a read query.
// This is a map from the attribute name to a pair of values.
// The first is number of elements (offsets) for var size attributes, and the
// second is number of elements in the data buffer. For fixed sized attributes
// (and coordinates), the first is always 0.
func (q *Query) ResultBufferElements() (map[string][2]uint64, error) {
	elements := make(map[string][2]uint64, 0)

	// Will need the schema to infer data type size for attributes
	schema, err := q.array.Schema()

	if err != nil {
		return nil, fmt.Errorf("Could not get schema for ResultBufferElements: %s", err)
	}

	for attributeName, v := range q.resultBufferElements {
		// Handle coordinates
		if attributeName == TILEDB_COORDS {
			// For fixed length attributes offset elements are always zero
			offsetElements := uint64(0)

			domain, err := schema.Domain()
			if err != nil {
				return nil, fmt.Errorf("Could not get domain for ResultBufferElements: %s", err)
			}
			domainType, err := domain.Type()
			if err != nil {
				return nil, fmt.Errorf("Could not get domainType for ResultBufferElements: %s", err)
			}
			domainTypeSize := uint64(C.tiledb_datatype_size(C.tiledb_datatype_t(domainType)))

			// Number of buffer elements is calculated
			bufferElements := (*v[1]) / domainTypeSize
			elements[attributeName] = [2]uint64{offsetElements, bufferElements}
		} else {
			// For fixed length attributes offset elements are always zero
			offsetElements := uint64(0)
			if v[0] != nil {
				// The attribute is variable lenght
				offsetElements = (*v[0]) / uint64(unsafe.Sizeof(uint64(0)))
			}

			// Get the attribute
			attribute, err := schema.AttributeFromName(attributeName)

			if err != nil {
				return nil, fmt.Errorf("Could not get attribute for ResultBufferElements: %s", err)
			}

			// Get datatype size to convert byte lengths to needed buffer sizes
			dataType, err := attribute.Type()
			if err != nil {
				return nil, fmt.Errorf("Could not get dataType for ResultBufferElements: %s", err)
			}
			dataTypeSize := uint64(C.tiledb_datatype_size(C.tiledb_datatype_t(dataType)))

			// Number of buffer elements is calculated
			bufferElements := (*v[1]) / dataTypeSize
			elements[attributeName] = [2]uint64{offsetElements, bufferElements}
		}
	}

	return elements, nil
}

// BufferVar returns a slice backed by the underlying c buffer from tiledb for
// offets and values
func (q *Query) BufferVar(attributeName string) ([]uint64, interface{}, error) {
	var datatype Datatype
	schema, err := q.array.Schema()
	if err != nil {
		return nil, nil, err
	}

	if attributeName == TILEDB_COORDS {
		domain, err := schema.Domain()
		if err != nil {
			return nil, nil, err
		}
		datatype, err = domain.Type()
		if err != nil {
			return nil, nil, err
		}
	} else {
		attribute, err := schema.AttributeFromName(attributeName)
		if err != nil {
			return nil, nil, err
		}
		datatype, err = attribute.Type()
		if err != nil {
			return nil, nil, err
		}
	}

	cattributeName := C.CString(attributeName)
	defer C.free(unsafe.Pointer(cattributeName))

	var ret C.int32_t
	var cbufferSize *C.uint64_t
	var cbuffer unsafe.Pointer
	var buffer interface{}
	var coffsetsSize *C.uint64_t
	var coffsets *C.uint64_t
	var offsets []uint64
	switch datatype {
	case TILEDB_INT8:
		ret = C.tiledb_query_get_buffer_var(q.context.tiledbContext, q.tiledbQuery, cattributeName, &coffsets, &coffsetsSize, &cbuffer, &cbufferSize)
		length := (*cbufferSize) / C.sizeof_int8_t
		buffer = (*[1 << 30]int8)(cbuffer)[:length:length]
		offsetsLength := *coffsetsSize / C.sizeof_uint64_t
		offsets = (*[1 << 30]uint64)(unsafe.Pointer(coffsets))[:offsetsLength:offsetsLength]

	case TILEDB_INT16:
		ret = C.tiledb_query_get_buffer_var(q.context.tiledbContext, q.tiledbQuery, cattributeName, &coffsets, &coffsetsSize, &cbuffer, &cbufferSize)
		length := (*cbufferSize) / C.sizeof_int16_t
		buffer = (*[1 << 30]int16)(cbuffer)[:length:length]
		offsetsLength := *coffsetsSize / C.sizeof_uint64_t
		offsets = (*[1 << 30]uint64)(unsafe.Pointer(coffsets))[:offsetsLength:offsetsLength]

	case TILEDB_INT32:
		ret = C.tiledb_query_get_buffer_var(q.context.tiledbContext, q.tiledbQuery, cattributeName, &coffsets, &coffsetsSize, &cbuffer, &cbufferSize)
		length := (*cbufferSize) / C.sizeof_int32_t
		buffer = (*[1 << 30]int32)(cbuffer)[:length:length]
		offsetsLength := *coffsetsSize / C.sizeof_uint64_t
		offsets = (*[1 << 30]uint64)(unsafe.Pointer(coffsets))[:offsetsLength:offsetsLength]

	case TILEDB_INT64:
		ret = C.tiledb_query_get_buffer_var(q.context.tiledbContext, q.tiledbQuery, cattributeName, &coffsets, &coffsetsSize, &cbuffer, &cbufferSize)
		length := (*cbufferSize) / C.sizeof_int64_t
		buffer = (*[1 << 30]int64)(cbuffer)[:length:length]
		offsetsLength := *coffsetsSize / C.sizeof_uint64_t
		offsets = (*[1 << 30]uint64)(unsafe.Pointer(coffsets))[:offsetsLength:offsetsLength]

	case TILEDB_UINT8:
		ret = C.tiledb_query_get_buffer_var(q.context.tiledbContext, q.tiledbQuery, cattributeName, &coffsets, &coffsetsSize, &cbuffer, &cbufferSize)
		length := (*cbufferSize) / C.sizeof_uint8_t
		buffer = (*[1 << 30]uint8)(cbuffer)[:length:length]
		offsetsLength := *coffsetsSize / C.sizeof_uint64_t
		offsets = (*[1 << 30]uint64)(unsafe.Pointer(coffsets))[:offsetsLength:offsetsLength]

	case TILEDB_UINT16:
		ret = C.tiledb_query_get_buffer_var(q.context.tiledbContext, q.tiledbQuery, cattributeName, &coffsets, &coffsetsSize, &cbuffer, &cbufferSize)
		length := (*cbufferSize) / C.sizeof_uint16_t
		buffer = (*[1 << 30]uint16)(cbuffer)[:length:length]
		offsetsLength := *coffsetsSize / C.sizeof_uint64_t
		offsets = (*[1 << 30]uint64)(unsafe.Pointer(coffsets))[:offsetsLength:offsetsLength]

	case TILEDB_UINT32:
		ret = C.tiledb_query_get_buffer_var(q.context.tiledbContext, q.tiledbQuery, cattributeName, &coffsets, &coffsetsSize, &cbuffer, &cbufferSize)
		length := (*cbufferSize) / C.sizeof_uint32_t
		buffer = (*[1 << 30]uint32)(cbuffer)[:length:length]
		offsetsLength := *coffsetsSize / C.sizeof_uint64_t
		offsets = (*[1 << 30]uint64)(unsafe.Pointer(coffsets))[:offsetsLength:offsetsLength]

	case TILEDB_UINT64:
		ret = C.tiledb_query_get_buffer_var(q.context.tiledbContext, q.tiledbQuery, cattributeName, &coffsets, &coffsetsSize, &cbuffer, &cbufferSize)
		length := (*cbufferSize) / C.sizeof_uint64_t
		buffer = (*[1 << 30]uint64)(cbuffer)[:length:length]
		offsetsLength := *coffsetsSize / C.sizeof_uint64_t
		offsets = (*[1 << 30]uint64)(unsafe.Pointer(coffsets))[:offsetsLength:offsetsLength]

	case TILEDB_FLOAT32:
		ret = C.tiledb_query_get_buffer_var(q.context.tiledbContext, q.tiledbQuery, cattributeName, &coffsets, &coffsetsSize, &cbuffer, &cbufferSize)
		length := (*cbufferSize) / C.sizeof_float
		buffer = (*[1 << 30]float32)(cbuffer)[:length:length]
		offsetsLength := *coffsetsSize / C.sizeof_uint64_t
		offsets = (*[1 << 30]uint64)(unsafe.Pointer(coffsets))[:offsetsLength:offsetsLength]

	case TILEDB_FLOAT64:
		ret = C.tiledb_query_get_buffer_var(q.context.tiledbContext, q.tiledbQuery, cattributeName, &coffsets, &coffsetsSize, &cbuffer, &cbufferSize)
		length := (*cbufferSize) / C.sizeof_double
		buffer = (*[1 << 30]float64)(cbuffer)[:length:length]
		offsetsLength := *coffsetsSize / C.sizeof_uint64_t
		offsets = (*[1 << 30]uint64)(unsafe.Pointer(coffsets))[:offsetsLength:offsetsLength]

	case TILEDB_CHAR:
		ret = C.tiledb_query_get_buffer_var(q.context.tiledbContext, q.tiledbQuery, cattributeName, &coffsets, &coffsetsSize, &cbuffer, &cbufferSize)
		length := (*cbufferSize) / C.sizeof_char
		buffer = (*[1 << 30]byte)(cbuffer)[:length:length]
		offsetsLength := *coffsetsSize / C.sizeof_uint64_t
		offsets = (*[1 << 30]uint64)(unsafe.Pointer(coffsets))[:offsetsLength:offsetsLength]

	case TILEDB_STRING_ASCII:
		ret = C.tiledb_query_get_buffer_var(q.context.tiledbContext, q.tiledbQuery, cattributeName, &coffsets, &coffsetsSize, &cbuffer, &cbufferSize)
		length := (*cbufferSize) / C.sizeof_uint8_t
		buffer = (*[1 << 30]uint8)(cbuffer)[:length:length]
		offsetsLength := *coffsetsSize / C.sizeof_uint64_t
		offsets = (*[1 << 30]uint64)(unsafe.Pointer(coffsets))[:offsetsLength:offsetsLength]

	case TILEDB_STRING_UTF8:
		ret = C.tiledb_query_get_buffer_var(q.context.tiledbContext, q.tiledbQuery, cattributeName, &coffsets, &coffsetsSize, &cbuffer, &cbufferSize)
		length := (*cbufferSize) / C.sizeof_uint8_t
		buffer = (*[1 << 30]uint8)(cbuffer)[:length:length]
		offsetsLength := *coffsetsSize / C.sizeof_uint64_t
		offsets = (*[1 << 30]uint64)(unsafe.Pointer(coffsets))[:offsetsLength:offsetsLength]

	case TILEDB_STRING_UTF16:
		ret = C.tiledb_query_get_buffer_var(q.context.tiledbContext, q.tiledbQuery, cattributeName, &coffsets, &coffsetsSize, &cbuffer, &cbufferSize)
		length := (*cbufferSize) / C.sizeof_uint16_t
		buffer = (*[1 << 30]uint16)(cbuffer)[:length:length]
		offsetsLength := *coffsetsSize / C.sizeof_uint64_t
		offsets = (*[1 << 30]uint64)(unsafe.Pointer(coffsets))[:offsetsLength:offsetsLength]

	case TILEDB_STRING_UTF32:
		ret = C.tiledb_query_get_buffer_var(q.context.tiledbContext, q.tiledbQuery, cattributeName, &coffsets, &coffsetsSize, &cbuffer, &cbufferSize)
		length := (*cbufferSize) / C.sizeof_uint32_t
		buffer = (*[1 << 30]uint32)(cbuffer)[:length:length]
		offsetsLength := *coffsetsSize / C.sizeof_uint64_t
		offsets = (*[1 << 30]uint64)(unsafe.Pointer(coffsets))[:offsetsLength:offsetsLength]

	case TILEDB_STRING_UCS2:
		ret = C.tiledb_query_get_buffer_var(q.context.tiledbContext, q.tiledbQuery, cattributeName, &coffsets, &coffsetsSize, &cbuffer, &cbufferSize)
		length := (*cbufferSize) / C.sizeof_uint16_t
		buffer = (*[1 << 30]uint16)(cbuffer)[:length:length]
		offsetsLength := *coffsetsSize / C.sizeof_uint64_t
		offsets = (*[1 << 30]uint64)(unsafe.Pointer(coffsets))[:offsetsLength:offsetsLength]

	case TILEDB_STRING_UCS4:
		ret = C.tiledb_query_get_buffer_var(q.context.tiledbContext, q.tiledbQuery, cattributeName, &coffsets, &coffsetsSize, &cbuffer, &cbufferSize)
		length := (*cbufferSize) / C.sizeof_uint32_t
		buffer = (*[1 << 30]uint32)(cbuffer)[:length:length]
		offsetsLength := *coffsetsSize / C.sizeof_uint64_t
		offsets = (*[1 << 30]uint64)(unsafe.Pointer(coffsets))[:offsetsLength:offsetsLength]

	case TILEDB_ANY:
		ret = C.tiledb_query_get_buffer_var(q.context.tiledbContext, q.tiledbQuery, cattributeName, &coffsets, &coffsetsSize, &cbuffer, &cbufferSize)
		length := (*cbufferSize) / C.sizeof_int32_t
		buffer = (*[1 << 30]C.int8_t)(cbuffer)[:length:length]
		offsetsLength := *coffsetsSize / C.sizeof_uint64_t
		offsets = (*[1 << 30]uint64)(unsafe.Pointer(coffsets))[:offsetsLength:offsetsLength]

	default:
		return nil, nil, fmt.Errorf("Unrecognized attribute type: %d", datatype)
	}
	if ret != C.TILEDB_OK {
		return nil, nil, fmt.Errorf("Error getting tiledb query buffer for %s: %s", attributeName, q.context.LastError())
	}

	return offsets, buffer, nil
}

// SetLayout sets the layout of the cells to be written or read
func (q *Query) SetLayout(layout Layout) error {
	ret := C.tiledb_query_set_layout(q.context.tiledbContext, q.tiledbQuery, C.tiledb_layout_t(layout))
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error setting query layout: %s", q.context.LastError())
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

// SetCoordinates sets the coordinate buffer
func (q *Query) SetCoordinates(coordinates interface{}) (*uint64, error) {
	return q.SetBuffer(TILEDB_COORDS, coordinates)
}
