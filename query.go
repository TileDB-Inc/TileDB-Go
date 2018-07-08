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
	"sync"
	"unsafe"
)

// Query construct and exeute read/write queries on a tiledb Array
type Query struct {
	tiledbQuery *C.tiledb_query_t
	array       *Array
	context     *Context
	uri         string
	buffers     []interface{}
	bufferMutex sync.Mutex
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

	return &query, nil
}

// Free tiledb_query_t that was allocated on heap in c
func (q *Query) Free() {
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
func (q *Query) SetBuffer(attribute string, buffer interface{}) error {
	bufferReflectType := reflect.TypeOf(buffer)
	bufferReflectValue := reflect.ValueOf(buffer)
	if bufferReflectValue.Kind() != reflect.Slice {
		return fmt.Errorf("Buffer passed must be a slice that is pre-allocated, type passed was: %s", bufferReflectValue.Kind().String())
	}

	// Next get the attribute to validate the buffer type is the same as the attribute
	schema, err := q.array.Schema()
	if err != nil {
		return fmt.Errorf("Could not get array schema for SetBuffer: %s", err)
	}

	schemaAttribute, err := schema.AttributeFromName(attribute)
	if err != nil {
		return fmt.Errorf("Could not get attribute for SetBuffer: %s", attribute)
	}

	attributeType, err := schemaAttribute.Type()
	if err != nil {
		return fmt.Errorf("Could not get attributeType for SetBuffer: %s", attribute)
	}

	bufferType := bufferReflectType.Elem().Kind()
	if attributeType.ReflectKind() != bufferType {
		return fmt.Errorf("Buffer and Attribute do not have the same data types. Buffer: %s, Attribute: %s", bufferType.String(), attributeType.ReflectKind().String())
	}

	var cbuffer unsafe.Pointer
	// Get length of slice, this will be multiplied by size of datatype below
	cbufferSize := C.uint64_t(uint64(bufferReflectValue.Len()))

	if cbufferSize == C.uint64_t(0) {
		return fmt.Errorf("Buffer has no length, buffers are required to be initialized before reading or writting")
	}

	// Acquire a lock to make appending to buffer slice thread safe
	q.bufferMutex.Lock()
	defer q.bufferMutex.Unlock()

	switch bufferType {
	case reflect.Int:
		// Set buffersize
		cbufferSize = cbufferSize * C.uint64_t(unsafe.Sizeof(int(0)))
		// Create buffer void*
		tmpBuffer := buffer.([]int)
		// Store slice so underlying array is not gc'ed
		q.buffers = append(q.buffers, tmpBuffer)
		cbuffer = unsafe.Pointer(&(tmpBuffer)[0])
	case reflect.Int8:
		// Set buffersize
		cbufferSize = cbufferSize * C.uint64_t(unsafe.Sizeof(int8(0)))
		// Create buffer void*
		tmpBuffer := buffer.([]int8)
		// Store slice so underlying array is not gc'ed
		q.buffers = append(q.buffers, tmpBuffer)
		cbuffer = unsafe.Pointer(&(tmpBuffer)[0])
	case reflect.Int16:
		// Set buffersize
		cbufferSize = cbufferSize * C.uint64_t(unsafe.Sizeof(int16(0)))
		// Create buffer void*
		tmpBuffer := buffer.([]int16)
		// Store slice so underlying array is not gc'ed
		q.buffers = append(q.buffers, tmpBuffer)
		cbuffer = unsafe.Pointer(&(tmpBuffer)[0])
	case reflect.Int32:
		// Set buffersize
		cbufferSize = cbufferSize * C.uint64_t(unsafe.Sizeof(int32(0)))
		// Create buffer void*
		tmpBuffer := buffer.([]int32)
		// Store slice so underlying array is not gc'ed
		q.buffers = append(q.buffers, tmpBuffer)
		cbuffer = unsafe.Pointer(&(tmpBuffer)[0])
	case reflect.Int64:
		// Set buffersize
		cbufferSize = cbufferSize * C.uint64_t(unsafe.Sizeof(int64(0)))
		// Create buffer void*
		tmpBuffer := buffer.([]int64)
		// Store slice so underlying array is not gc'ed
		q.buffers = append(q.buffers, tmpBuffer)
		cbuffer = unsafe.Pointer(&(tmpBuffer)[0])
	case reflect.Uint:
		// Set buffersize
		cbufferSize = cbufferSize * C.uint64_t(unsafe.Sizeof(uint(0)))
		// Create buffer void*
		tmpBuffer := buffer.([]uint)
		// Store slice so underlying array is not gc'ed
		q.buffers = append(q.buffers, tmpBuffer)
		cbuffer = unsafe.Pointer(&(tmpBuffer)[0])
	case reflect.Uint8:
		// Set buffersize
		cbufferSize = cbufferSize * C.uint64_t(unsafe.Sizeof(uint8(0)))
		// Create buffer void*
		tmpBuffer := buffer.([]uint8)
		// Store slice so underlying array is not gc'ed
		q.buffers = append(q.buffers, tmpBuffer)
		cbuffer = unsafe.Pointer(&(tmpBuffer)[0])
	case reflect.Uint16:
		// Set buffersize
		cbufferSize = cbufferSize * C.uint64_t(unsafe.Sizeof(uint16(0)))
		// Create buffer void*
		tmpBuffer := buffer.([]uint16)
		// Store slice so underlying array is not gc'ed
		q.buffers = append(q.buffers, tmpBuffer)
		cbuffer = unsafe.Pointer(&(tmpBuffer)[0])
	case reflect.Uint32:
		// Set buffersize
		cbufferSize = cbufferSize * C.uint64_t(unsafe.Sizeof(uint32(0)))
		// Create buffer void*
		tmpBuffer := buffer.([]uint32)
		// Store slice so underlying array is not gc'ed
		q.buffers = append(q.buffers, tmpBuffer)
		cbuffer = unsafe.Pointer(&(tmpBuffer)[0])
	case reflect.Uint64:
		// Set buffersize
		cbufferSize = cbufferSize * C.uint64_t(unsafe.Sizeof(uint64(0)))
		// Create buffer void*
		tmpBuffer := buffer.([]uint64)
		// Store slice so underlying array is not gc'ed
		q.buffers = append(q.buffers, tmpBuffer)
		cbuffer = unsafe.Pointer(&(tmpBuffer)[0])
	case reflect.Float32:
		// Set buffersize
		cbufferSize = cbufferSize * C.uint64_t(unsafe.Sizeof(float32(0)))
		// Create buffer void*
		tmpBuffer := buffer.([]float32)
		// Store slice so underlying array is not gc'ed
		q.buffers = append(q.buffers, tmpBuffer)
		cbuffer = unsafe.Pointer(&(tmpBuffer)[0])
	case reflect.Float64:
		// Set buffersize
		cbufferSize = cbufferSize * C.uint64_t(unsafe.Sizeof(float64(0)))
		// Create buffer void*
		tmpBuffer := buffer.([]float64)
		// Store slice so underlying array is not gc'ed
		q.buffers = append(q.buffers, tmpBuffer)
		cbuffer = unsafe.Pointer(&(tmpBuffer)[0])
	default:
		return fmt.Errorf("Unrecognized buffer type passed: %s", bufferType.String())
	}

	cAttribute := C.CString(attribute)
	defer C.free(unsafe.Pointer(cAttribute))
	ret := C.tiledb_query_set_buffer(q.context.tiledbContext, q.tiledbQuery, cAttribute, cbuffer, &cbufferSize)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error setting query buffer: %s", q.context.LastError())
	}
	return nil
}

// SetBufferVar Sets the buffer for a fixed-sized attribute to a query
// The buffer must be an initialized slice
func (q *Query) SetBufferVar(attribute string, offset []uint64, buffer interface{}) error {
	bufferReflectType := reflect.TypeOf(buffer)
	bufferReflectValue := reflect.ValueOf(buffer)
	if bufferReflectValue.Kind() != reflect.Slice {
		return fmt.Errorf("Buffer passed must be a slice that is pre-allocated, type passed was: %s", bufferReflectValue.Kind().String())
	}

	// Next get the attribute to validate the buffer type is the same as the attribute
	schema, err := q.array.Schema()
	if err != nil {
		return fmt.Errorf("Could not get array schema for SetBuffer: %s", err)
	}

	schemaAttribute, err := schema.AttributeFromName(attribute)
	if err != nil {
		return fmt.Errorf("Could not get attribute for SetBuffer: %s", attribute)
	}

	attributeType, err := schemaAttribute.Type()
	if err != nil {
		return fmt.Errorf("Could not get attributeType for SetBuffer: %s", attribute)
	}

	bufferType := bufferReflectType.Elem().Kind()
	if attributeType.ReflectKind() != bufferType {
		return fmt.Errorf("Buffer and Attribute do not have the same data types. Buffer: %s, Attribute: %s", bufferType.String(), attributeType.ReflectKind().String())
	}

	cbufferSize := C.uint64_t(uint64(bufferReflectValue.Len()))

	if cbufferSize == C.uint64_t(0) {
		return fmt.Errorf("Buffer has no length, buffers are required to be initialized before reading or writting")
	}

	coffsetSize := C.uint64_t(uint64(len(offset)) * uint64(unsafe.Sizeof(uint64(0))))

	if coffsetSize == C.uint64_t(0) {
		return fmt.Errorf("Offset slice has no length, offset slices are required to be initialized before reading or writting")
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
		cbufferSize = cbufferSize * C.uint64_t(unsafe.Sizeof(int(0)))

		// Create buffer void*
		tmpBuffer := buffer.([]int)
		// Store slice so underlying array is not gc'ed
		q.buffers = append(q.buffers, tmpBuffer)
		cbuffer = unsafe.Pointer(&(tmpBuffer)[0])
	case reflect.Int8:
		// Set buffersize
		cbufferSize = cbufferSize * C.uint64_t(unsafe.Sizeof(int8(0)))

		// Create buffer void*
		tmpBuffer := buffer.([]int8)
		// Store slice so underlying array is not gc'ed
		q.buffers = append(q.buffers, tmpBuffer)
		cbuffer = unsafe.Pointer(&(tmpBuffer)[0])
	case reflect.Int16:
		// Set buffersize
		cbufferSize = cbufferSize * C.uint64_t(unsafe.Sizeof(int16(0)))

		// Create buffer void*
		tmpBuffer := buffer.([]int16)
		// Store slice so underlying array is not gc'ed
		q.buffers = append(q.buffers, tmpBuffer)
		cbuffer = unsafe.Pointer(&(tmpBuffer)[0])
	case reflect.Int32:
		// Set buffersize
		cbufferSize = cbufferSize * C.uint64_t(unsafe.Sizeof(int32(0)))

		// Create buffer void*
		tmpBuffer := buffer.([]int32)
		// Store slice so underlying array is not gc'ed
		q.buffers = append(q.buffers, tmpBuffer)
		cbuffer = unsafe.Pointer(&(tmpBuffer)[0])
	case reflect.Int64:
		// Set buffersize
		cbufferSize = cbufferSize * C.uint64_t(unsafe.Sizeof(int64(0)))

		// Create buffer void*
		tmpBuffer := buffer.([]int64)
		// Store slice so underlying array is not gc'ed
		q.buffers = append(q.buffers, tmpBuffer)
		cbuffer = unsafe.Pointer(&(tmpBuffer)[0])
	case reflect.Uint:
		// Set buffersize
		cbufferSize = cbufferSize * C.uint64_t(unsafe.Sizeof(uint(0)))

		// Create buffer void*
		tmpBuffer := buffer.([]uint)
		// Store slice so underlying array is not gc'ed
		q.buffers = append(q.buffers, tmpBuffer)
		cbuffer = unsafe.Pointer(&(tmpBuffer)[0])
	case reflect.Uint8:
		// Set buffersize
		cbufferSize = cbufferSize * C.uint64_t(unsafe.Sizeof(uint8(0)))

		// Create buffer void*
		tmpBuffer := buffer.([]uint8)
		// Store slice so underlying array is not gc'ed
		q.buffers = append(q.buffers, tmpBuffer)
		cbuffer = unsafe.Pointer(&(tmpBuffer)[0])
	case reflect.Uint16:
		// Set buffersize
		cbufferSize = cbufferSize * C.uint64_t(unsafe.Sizeof(uint16(0)))

		// Create buffer void*
		tmpBuffer := buffer.([]uint16)
		// Store slice so underlying array is not gc'ed
		q.buffers = append(q.buffers, tmpBuffer)
		cbuffer = unsafe.Pointer(&(tmpBuffer)[0])
	case reflect.Uint32:
		// Set buffersize
		cbufferSize = cbufferSize * C.uint64_t(unsafe.Sizeof(uint32(0)))

		// Create buffer void*
		tmpBuffer := buffer.([]uint32)
		// Store slice so underlying array is not gc'ed
		q.buffers = append(q.buffers, tmpBuffer)
		cbuffer = unsafe.Pointer(&(tmpBuffer)[0])
	case reflect.Uint64:
		// Set buffersize
		cbufferSize = cbufferSize * C.uint64_t(unsafe.Sizeof(uint64(0)))

		// Create buffer void*
		tmpBuffer := buffer.([]uint64)
		// Store slice so underlying array is not gc'ed
		q.buffers = append(q.buffers, tmpBuffer)
		cbuffer = unsafe.Pointer(&(tmpBuffer)[0])
	case reflect.Float32:
		// Set buffersize
		cbufferSize = cbufferSize * C.uint64_t(unsafe.Sizeof(float32(0)))

		// Create buffer void*
		tmpBuffer := buffer.([]float32)
		// Store slice so underlying array is not gc'ed
		q.buffers = append(q.buffers, tmpBuffer)
		cbuffer = unsafe.Pointer(&(tmpBuffer)[0])
	case reflect.Float64:
		// Set buffersize
		cbufferSize = cbufferSize * C.uint64_t(unsafe.Sizeof(float64(0)))

		// Create buffer void*
		tmpBuffer := buffer.([]float64)
		// Store slice so underlying array is not gc'ed
		q.buffers = append(q.buffers, tmpBuffer)
		cbuffer = unsafe.Pointer(&(tmpBuffer)[0])
	default:
		return fmt.Errorf("Unrecognized buffer type passed: %s", bufferType.String())
	}

	cAttribute := C.CString(attribute)
	defer C.free(unsafe.Pointer(cAttribute))
	ret := C.tiledb_query_set_buffer_var(q.context.tiledbContext, q.tiledbQuery, cAttribute, (*C.uint64_t)(coffset), &coffsetSize, cbuffer, &cbufferSize)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error setting query var buffer: %s", q.context.LastError())
	}
	return nil
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
	var hasResults C.int
	ret := C.tiledb_query_has_results(q.context.tiledbContext, q.tiledbQuery, &hasResults)
	if ret != C.TILEDB_OK {
		return false, fmt.Errorf("Error checking if query has results: %s", q.context.LastError())
	}
	return int(hasResults) == 1, nil
}
