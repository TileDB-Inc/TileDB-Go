package tiledb

/*
#include <tiledb/tiledb.h>
#include <tiledb/tiledb_serialization.h>
#include <stdio.h>
#include <stdlib.h>
*/
import "C"

import (
	"fmt"
	"reflect"
	"runtime"
	"unsafe"
)

// SerializeArraySchema serializes an array schema
func SerializeArraySchema(schema *ArraySchema, serializationType SerializationType, clientSide bool) ([]byte, error) {
	var cClientSide C.int32_t
	if clientSide {
		cClientSide = 1
	} else {
		cClientSide = 0
	}

	buffer := Buffer{context: schema.context}
	freeOnGC(&buffer)

	ret := C.tiledb_serialize_array_schema(schema.context.tiledbContext, schema.tiledbArraySchema, C.tiledb_serialization_type_t(serializationType), cClientSide, &buffer.tiledbBuffer)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error serializing array schema: %s", schema.context.LastError())
	}

	return buffer.Serialize(serializationType)
}

// DeserializeArraySchema deserializes a new array schema from the given buffer
func DeserializeArraySchema(buffer *Buffer, serializationType SerializationType, clientSide bool) (*ArraySchema, error) {
	schema := ArraySchema{context: buffer.context}

	var cClientSide C.int32_t
	if clientSide {
		cClientSide = 1
	} else {
		cClientSide = 0
	}

	ret := C.tiledb_deserialize_array_schema(schema.context.tiledbContext, buffer.tiledbBuffer, C.tiledb_serialization_type_t(serializationType), cClientSide, &schema.tiledbArraySchema)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error deserializing array schema: %s", schema.context.LastError())
	}

	// This needs to happen *after* the tiledb_deserialize_array_schema call
	// because that may leave the arraySchema with a non-nil pointer
	// to already-freed memory.
	freeOnGC(&schema)

	return &schema, nil
}

// SerializeArrayNonEmptyDomain gets and serializes the array nonempty domain
func SerializeArrayNonEmptyDomain(a *Array, serializationType SerializationType) ([]byte, error) {
	schema, err := a.Schema()
	if err != nil {
		return nil, err
	}
	domain, err := schema.Domain()
	if err != nil {
		return nil, err
	}
	domainType, err := domain.Type()
	if err != nil {
		return nil, err
	}
	ndims, err := domain.NDim()
	if err != nil {
		return nil, err
	}
	subarraySize := 2 * ndims * uint(domainType.Size())

	var isEmpty C.int32_t
	tmpDomain := make([]uint8, subarraySize)
	ret := C.tiledb_array_get_non_empty_domain(a.context.tiledbContext, a.tiledbArray, unsafe.Pointer(&tmpDomain[0]), &isEmpty)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error serializing array nonempty domain: %s", a.context.LastError())
	}

	buffer := Buffer{context: schema.context}
	freeOnGC(&buffer)

	var cClientSide = C.int32_t(0) // Currently this parameter is unused in libtiledb
	ret = C.tiledb_serialize_array_nonempty_domain(a.context.tiledbContext, a.tiledbArray, unsafe.Pointer(&tmpDomain[0]), isEmpty, C.tiledb_serialization_type_t(serializationType), cClientSide, &buffer.tiledbBuffer)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error serializing array nonempty domain: %s", a.context.LastError())
	}

	return buffer.Serialize(serializationType)
}

// DeserializeArrayNonEmptyDomain deserializes an array nonempty domain
func DeserializeArrayNonEmptyDomain(a *Array, buffer *Buffer, serializationType SerializationType) ([]NonEmptyDomain, bool, error) {
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

	tmpDomain, tmpDomainPtr, err := domainType.MakeSlice(uint64(2 * ndims))
	if err != nil {
		return nil, false, err
	}

	var cClientSide = C.int32_t(0) // Currently this parameter is unused in libtiledb
	var isEmpty C.int32_t
	ret := C.tiledb_deserialize_array_nonempty_domain(a.context.tiledbContext, a.tiledbArray, buffer.tiledbBuffer, C.tiledb_serialization_type_t(serializationType), cClientSide, tmpDomainPtr, &isEmpty)
	if ret != C.TILEDB_OK {
		return nil, false, fmt.Errorf("Error serializing array nonempty domain: %s", a.context.LastError())
	}

	if isEmpty == 1 {
		return nil, true, nil
	}

	nonEmptyDomains := make([]NonEmptyDomain, ndims)
	for i := range nonEmptyDomains {
		dimension, err := domain.DimensionFromIndex(uint(i))
		if err != nil {
			return nil, false, err
		}

		var nonEmptyDomain *NonEmptyDomain

		switch tmpDomain := tmpDomain.(type) {
		case []int:
			nonEmptyDomain, err = getNonEmptyDomainForDim(dimension, tmpDomain[2*i:2*i+2])
		case []int8:
			nonEmptyDomain, err = getNonEmptyDomainForDim(dimension, tmpDomain[2*i:2*i+2])
		case []int16:
			nonEmptyDomain, err = getNonEmptyDomainForDim(dimension, tmpDomain[2*i:2*i+2])
		case []int32:
			nonEmptyDomain, err = getNonEmptyDomainForDim(dimension, tmpDomain[2*i:2*i+2])
		case []int64:
			nonEmptyDomain, err = getNonEmptyDomainForDim(dimension, tmpDomain[2*i:2*i+2])
		case []uint:
			nonEmptyDomain, err = getNonEmptyDomainForDim(dimension, tmpDomain[2*i:2*i+2])
		case []uint8:
			nonEmptyDomain, err = getNonEmptyDomainForDim(dimension, tmpDomain[2*i:2*i+2])
		case []uint16:
			nonEmptyDomain, err = getNonEmptyDomainForDim(dimension, tmpDomain[2*i:2*i+2])
		case []uint32:
			nonEmptyDomain, err = getNonEmptyDomainForDim(dimension, tmpDomain[2*i:2*i+2])
		case []uint64:
			nonEmptyDomain, err = getNonEmptyDomainForDim(dimension, tmpDomain[2*i:2*i+2])
		case []float32:
			nonEmptyDomain, err = getNonEmptyDomainForDim(dimension, tmpDomain[2*i:2*i+2])
		case []float64:
			nonEmptyDomain, err = getNonEmptyDomainForDim(dimension, tmpDomain[2*i:2*i+2])
		case []bool:
			nonEmptyDomain, err = getNonEmptyDomainForDim(dimension, tmpDomain[2*i:2*i+2])
		}

		if err != nil {
			return nil, false, err
		}
		nonEmptyDomains[i] = *nonEmptyDomain
	}

	return nonEmptyDomains, false, nil
}

// SerializeArrayNonEmptyDomainAllDimensions gets and serializes the array nonempty domain
func SerializeArrayNonEmptyDomainAllDimensions(a *Array, serializationType SerializationType) ([]byte, error) {

	buffer := Buffer{context: a.context}
	freeOnGC(&buffer)

	var cClientSide = C.int32_t(0) // Currently this parameter is unused in libtiledb
	ret := C.tiledb_serialize_array_non_empty_domain_all_dimensions(a.context.tiledbContext, a.tiledbArray, C.tiledb_serialization_type_t(serializationType), cClientSide, &buffer.tiledbBuffer)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error serializing array nonempty domain: %s", a.context.LastError())
	}

	return buffer.Serialize(serializationType)
}

// DeserializeArrayNonEmptyDomainAllDimensions deserializes an array nonempty domain
func DeserializeArrayNonEmptyDomainAllDimensions(a *Array, buffer *Buffer, serializationType SerializationType) error {

	var cClientSide = C.int32_t(0) // Currently this parameter is unused in libtiledb
	ret := C.tiledb_deserialize_array_non_empty_domain_all_dimensions(a.context.tiledbContext, a.tiledbArray, buffer.tiledbBuffer, C.tiledb_serialization_type_t(serializationType), cClientSide)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error deserializing array nonempty domain: %s", a.context.LastError())
	}

	return nil
}

// SerializeArrayMaxBufferSizes gets and serializes the array max buffer sizes for the given subarray
func SerializeArrayMaxBufferSizes(a *Array, subarray interface{}, serializationType SerializationType) ([]byte, error) {
	// Create subarray void*
	var cSubarray unsafe.Pointer
	if reflect.TypeOf(subarray).Kind() != reflect.Slice {
		return nil, fmt.Errorf("subarray passed must be a slice, type passed was: %s", reflect.TypeOf(subarray).Kind().String())
	}
	switch subarray := subarray.(type) {
	case []int:
		cSubarray = slicePtr(subarray)
	case []int8:
		cSubarray = slicePtr(subarray)
	case []int16:
		cSubarray = slicePtr(subarray)
	case []int32:
		cSubarray = slicePtr(subarray)
	case []int64:
		cSubarray = slicePtr(subarray)
	case []uint:
		cSubarray = slicePtr(subarray)
	case []uint8:
		cSubarray = slicePtr(subarray)
	case []uint16:
		cSubarray = slicePtr(subarray)
	case []uint32:
		cSubarray = slicePtr(subarray)
	case []uint64:
		cSubarray = slicePtr(subarray)
	case []float32:
		cSubarray = slicePtr(subarray)
	case []float64:
		cSubarray = slicePtr(subarray)
	case []bool:
		cSubarray = slicePtr(subarray)
	default:
		return nil, fmt.Errorf("subarray must be a slice of scalars, not a %T", subarray)
	}

	buffer := Buffer{context: a.context}
	freeOnGC(&buffer)

	ret := C.tiledb_serialize_array_max_buffer_sizes(a.context.tiledbContext, a.tiledbArray, cSubarray, C.tiledb_serialization_type_t(serializationType), &buffer.tiledbBuffer)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("error serializing array max buffer sizes: %s", a.context.LastError())
	}

	return buffer.Serialize(serializationType)
}

// SerializeQuery serializes a query
func SerializeQuery(query *Query, serializationType SerializationType, clientSide bool) (*BufferList, error) {
	bufferList := BufferList{context: query.context}
	freeOnGC(&bufferList)

	var cClientSide C.int32_t
	if clientSide {
		cClientSide = 1
	} else {
		cClientSide = 0
	}

	ret := C.tiledb_serialize_query(query.context.tiledbContext, query.tiledbQuery, C.tiledb_serialization_type_t(serializationType), cClientSide, &bufferList.tiledbBufferList)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error serializing query: %s", query.context.LastError())
	}

	return &bufferList, nil
}

// DeserializeQuery deserializes a buffer into an existing query
func DeserializeQuery(query *Query, buffer *Buffer, serializationType SerializationType, clientSide bool) error {
	var cClientSide C.int32_t
	if clientSide {
		cClientSide = 1
	} else {
		cClientSide = 0
	}

	ret := C.tiledb_deserialize_query(query.context.tiledbContext, buffer.tiledbBuffer, C.tiledb_serialization_type_t(serializationType), cClientSide, query.tiledbQuery)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error deserializing query: %s", query.context.LastError())
	}

	return nil
}

// SerializeArrayMetadata gets and serializes the array metadata
func SerializeArrayMetadata(a *Array, serializationType SerializationType) ([]byte, error) {
	buffer := Buffer{context: a.context}
	freeOnGC(&buffer)

	ret := C.tiledb_serialize_array_metadata(a.context.tiledbContext, a.tiledbArray, C.tiledb_serialization_type_t(serializationType), &buffer.tiledbBuffer)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error serializing array metadata: %s", a.context.LastError())
	}

	return buffer.Serialize(serializationType)
}

// DeserializeArrayMetadata deserializes array metadata
func DeserializeArrayMetadata(a *Array, buffer *Buffer, serializationType SerializationType) error {
	ret := C.tiledb_deserialize_array_metadata(a.context.tiledbContext, a.tiledbArray, C.tiledb_serialization_type_t(serializationType), buffer.tiledbBuffer)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error deserializing array metadata: %s", a.context.LastError())
	}
	return nil
}

// SerializeQueryEstResultSizes gets and serializes the query estimated result sizes
func SerializeQueryEstResultSizes(q *Query, serializationType SerializationType, clientSide bool) ([]byte, error) {
	var cClientSide C.int32_t
	if clientSide {
		cClientSide = 1
	} else {
		cClientSide = 0
	}

	buffer := Buffer{context: q.context}
	freeOnGC(&buffer)

	ret := C.tiledb_serialize_query_est_result_sizes(q.context.tiledbContext, q.tiledbQuery, C.tiledb_serialization_type_t(serializationType), cClientSide, &buffer.tiledbBuffer)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error serializing query est buffer sizes: %s", q.context.LastError())
	}

	return buffer.Serialize(serializationType)
}

// DeserializeQueryEstResultSizes deserializes query estimated result sizes
func DeserializeQueryEstResultSizes(q *Query, buffer *Buffer, serializationType SerializationType, clientSide bool) error {
	var cClientSide C.int32_t
	if clientSide {
		cClientSide = 1
	} else {
		cClientSide = 0
	}

	ret := C.tiledb_deserialize_query_est_result_sizes(q.context.tiledbContext, q.tiledbQuery, C.tiledb_serialization_type_t(serializationType), cClientSide, buffer.tiledbBuffer)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error deserializing query est buffer sizes: %s", q.context.LastError())
	}
	return nil
}

// SerializeArray serializes an array
func SerializeArray(array *Array, serializationType SerializationType, clientSide bool) ([]byte, error) {
	var cClientSide C.int32_t
	if clientSide {
		cClientSide = 1
	} else {
		cClientSide = 0
	}

	buffer := Buffer{context: array.context}
	// Set finalizer for free C pointer on gc
	freeOnGC(&buffer)

	ret := C.tiledb_serialize_array(array.context.tiledbContext, array.tiledbArray, C.tiledb_serialization_type_t(serializationType), cClientSide, &buffer.tiledbBuffer)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("error serializing array: %s", array.context.LastError())
	}

	return buffer.Serialize(serializationType)
}

// DeserializeArray deserializes a new array from the given buffer
func DeserializeArray(buffer *Buffer, serializationType SerializationType, clientSide bool) (*Array, error) {
	array := Array{context: buffer.context}

	var cClientSide C.int32_t
	if clientSide {
		cClientSide = 1
	} else {
		cClientSide = 0
	}

	ret := C.tiledb_deserialize_array(array.context.tiledbContext, buffer.tiledbBuffer, C.tiledb_serialization_type_t(serializationType), cClientSide, &array.tiledbArray)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("error deserializing array: %s", array.context.LastError())
	}

	// Set finalizer for free C pointer on gc
	// This needs to happen *after* the tiledb_deserialize_array call
	// because that may leave the array with a non-nil pointer
	// to already-freed memory.
	freeOnGC(&array)

	return &array, nil
}

// SerializeFragmentInfo serializes fragment info
func SerializeFragmentInfo(fragmentInfo *FragmentInfo, serializationType SerializationType, clientSide bool) ([]byte, error) {
	var cClientSide C.int32_t
	if clientSide {
		cClientSide = 1
	} else {
		cClientSide = 0
	}

	buffer := Buffer{context: fragmentInfo.context}
	// Set finalizer for free C pointer on gc
	freeOnGC(&buffer)

	ret := C.tiledb_serialize_fragment_info(fragmentInfo.context.tiledbContext, fragmentInfo.tiledbFragmentInfo, C.tiledb_serialization_type_t(serializationType), cClientSide, &buffer.tiledbBuffer)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("error serializing array: %s", fragmentInfo.context.LastError())
	}

	return buffer.Serialize(serializationType)
}

// DeserializeFragmentInfo deserializes an existing fragment info from the given buffer
func DeserializeFragmentInfo(fragmentInfo FragmentInfo, buffer *Buffer, arrayURI string, serializationType SerializationType, clientSide bool) error {
	var cClientSide C.int32_t
	if clientSide {
		cClientSide = 1
	} else {
		cClientSide = 0
	}

	cArrayURI := C.CString(arrayURI)
	defer C.free(unsafe.Pointer(cArrayURI))

	ret := C.tiledb_deserialize_fragment_info(fragmentInfo.context.tiledbContext, buffer.tiledbBuffer, C.tiledb_serialization_type_t(serializationType), cArrayURI, cClientSide, fragmentInfo.tiledbFragmentInfo)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("error deserializing array: %s", fragmentInfo.context.LastError())
	}

	return nil
}

// SerializeFragmentInfoRequest serializes fragment info
func SerializeFragmentInfoRequest(fragmentInfo *FragmentInfo, serializationType SerializationType, clientSide bool) ([]byte, error) {
	var cClientSide C.int32_t
	if clientSide {
		cClientSide = 1
	} else {
		cClientSide = 0
	}

	buffer := Buffer{context: fragmentInfo.context}
	// Set finalizer for free C pointer on gc
	freeOnGC(&buffer)

	ret := C.tiledb_serialize_fragment_info_request(fragmentInfo.context.tiledbContext, fragmentInfo.tiledbFragmentInfo, C.tiledb_serialization_type_t(serializationType), cClientSide, &buffer.tiledbBuffer)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("error serializing array: %s", fragmentInfo.context.LastError())
	}

	return buffer.Serialize(serializationType)
}

// DeserializeFragmentInfoRequest deserializes an existing fragment info from the given buffer
func DeserializeFragmentInfoRequest(fragmentInfo FragmentInfo, buffer *Buffer, serializationType SerializationType, clientSide bool) error {
	var cClientSide C.int32_t
	if clientSide {
		cClientSide = 1
	} else {
		cClientSide = 0
	}

	ret := C.tiledb_deserialize_fragment_info_request(fragmentInfo.context.tiledbContext, buffer.tiledbBuffer, C.tiledb_serialization_type_t(serializationType), cClientSide, fragmentInfo.tiledbFragmentInfo)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("error deserializing array: %s", fragmentInfo.context.LastError())
	}

	return nil
}

func DeserializeQueryAndArray(context *Context, buffer *Buffer, serializationType SerializationType, clientSide bool, arrayURI string) (*Array, *Query, error) {
	var cClientSide C.int32_t
	if clientSide {
		cClientSide = 1
	} else {
		cClientSide = 0
	}

	cArrayURI := C.CString(arrayURI)
	defer C.free(unsafe.Pointer(cArrayURI))

	array := &Array{
		context: context,
	}

	query := &Query{
		context: context,
		array:   array,
	}

	ret := C.tiledb_deserialize_query_and_array(context.tiledbContext, buffer.tiledbBuffer, C.tiledb_serialization_type_t(serializationType), cClientSide, cArrayURI, &query.tiledbQuery, &array.tiledbArray)
	if ret != C.TILEDB_OK {
		return nil, nil, fmt.Errorf("error deserializing query: %s", context.LastError())
	}

	freeOnGC(array)
	freeOnGC(query)

	query.resultBufferElements = make(map[string][3]*uint64)

	// Make sure the buffer stays alive untill after the deserialization is complete
	runtime.KeepAlive(buffer)
	return array, query, nil
}

// HandleArrayDeleteFragmentsTimestampsRequest is used by TileDB cloud to handle DeleteFragments with tiledb:// uris
func HandleArrayDeleteFragmentsTimestampsRequest(context *Context, array *Array, buffer *Buffer, serializationType SerializationType) error {
	ret := C.tiledb_handle_array_delete_fragments_timestamps_request(context.tiledbContext, array.tiledbArray,
		C.tiledb_serialization_type_t(serializationType), buffer.tiledbBuffer)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("error deserializing delete fragments timestamps: %s", context.LastError())
	}

	runtime.KeepAlive(buffer)
	return nil
}

// HandleArrayDeleteFragmentsListRequest is used by TileDB cloud to handle DeleteFragmentsList with tiledb:// uris
func HandleArrayDeleteFragmentsListRequest(context *Context, array *Array, buffer *Buffer, serializationType SerializationType) error {
	ret := C.tiledb_handle_array_delete_fragments_list_request(context.tiledbContext, array.tiledbArray,
		C.tiledb_serialization_type_t(serializationType), buffer.tiledbBuffer)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("error deserializing delete fragments list: %s", context.LastError())
	}

	runtime.KeepAlive(buffer)
	return nil
}

// HandleQueryPlanRequest handles a request for a query plan. This is used by TileDB-Cloud
// It returns a buffer with the serialized response. The caller should free the buffer after use.
func HandleQueryPlanRequest(array *Array, serializationType SerializationType, request *Buffer) (*Buffer, error) {
	opContext := array.context

	response, err := NewBuffer(opContext)
	if err != nil {
		return nil, fmt.Errorf("Error allocating tiledb buffer: %s", opContext.LastError())
	}

	ret := C.tiledb_handle_query_plan_request(opContext.tiledbContext, array.tiledbArray, C.tiledb_serialization_type_t(serializationType),
		request.tiledbBuffer, response.tiledbBuffer)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error handling query plan request: %s", opContext.LastError())
	}

	runtime.KeepAlive(request)
	runtime.KeepAlive(array)

	return response, nil
}

// HandleConsolidationPlanRequest handles a request for a consolidation plan. This is used by TileDB-Cloud
// It returns a buffer with the serialized response. The caller should free the buffer after use.
func HandleConsolidationPlanRequest(array *Array, serializationType SerializationType, request *Buffer) (*Buffer, error) {
	opContext := array.context

	response, err := NewBuffer(opContext)
	if err != nil {
		return nil, fmt.Errorf("Error allocating tiledb buffer: %s", opContext.LastError())
	}

	ret := C.tiledb_handle_consolidation_plan_request(opContext.tiledbContext, array.tiledbArray, C.tiledb_serialization_type_t(serializationType),
		request.tiledbBuffer, response.tiledbBuffer)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error handling consolidation plan request: %s", opContext.LastError())
	}

	runtime.KeepAlive(request)
	runtime.KeepAlive(array)

	return response, nil
}
