package tiledb

/*
#include <tiledb/tiledb.h>
#include <tiledb/tiledb_serialization.h>
#include <stdio.h>
#include <stdlib.h>
*/
import "C"

import (
	"errors"
	"fmt"
	"runtime"
	"unsafe"
)

// SerializeArraySchemaToBuffer serializes an array schema and returns a Buffer object containing the payload.
func SerializeArraySchemaToBuffer(schema *ArraySchema, serializationType SerializationType, clientSide bool) (*Buffer, error) {
	var cClientSide C.int32_t
	if clientSide {
		cClientSide = 1
	} else {
		cClientSide = 0
	}

	var bufferPtr *C.tiledb_buffer_t
	ret := C.tiledb_serialize_array_schema(schema.context.tiledbContext.Get(), schema.tiledbArraySchema.Get(), C.tiledb_serialization_type_t(serializationType), cClientSide, &bufferPtr)
	runtime.KeepAlive(schema)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("error serializing array schema: %w", schema.context.LastError())
	}

	return newBufferFromHandle(schema.context, newBufferHandle(bufferPtr)), nil
}

// SerializeArraySchema serializes an array schema.
//
// Deprecated: Use SerializeArraySchemaToBuffer instead.
func SerializeArraySchema(schema *ArraySchema, serializationType SerializationType, clientSide bool) ([]byte, error) {
	buffer, err := SerializeArraySchemaToBuffer(schema, serializationType, clientSide)
	if err != nil {
		return nil, err
	}

	return buffer.Serialize(serializationType)
}

// DeserializeArraySchema deserializes a new array schema from the given buffer.
func DeserializeArraySchema(buffer *Buffer, serializationType SerializationType, clientSide bool) (*ArraySchema, error) {
	var cClientSide C.int32_t
	if clientSide {
		cClientSide = 1
	} else {
		cClientSide = 0
	}

	var arraySchemaPtr *C.tiledb_array_schema_t
	ret := C.tiledb_deserialize_array_schema(buffer.context.tiledbContext.Get(), buffer.tiledbBuffer.Get(), C.tiledb_serialization_type_t(serializationType), cClientSide, &arraySchemaPtr)
	runtime.KeepAlive(buffer)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("error deserializing array schema: %w", buffer.context.LastError())
	}

	return newArraySchemaFromHandle(buffer.context, newArraySchemaHandle(arraySchemaPtr)), nil
}

// SerializeArraySchemaEvolution serializes the given array schema evolution and serializes the group metadata and returns a Buffer object containing the payload.
func SerializeArraySchemaEvolutionToBuffer(arraySchemaEvolution *ArraySchemaEvolution, serializationType SerializationType, clientSide bool) (*Buffer, error) {
	var cClientSide C.int32_t
	if clientSide {
		cClientSide = 1
	} else {
		cClientSide = 0
	}

	var bufferPtr *C.tiledb_buffer_t
	ret := C.tiledb_serialize_array_schema_evolution(
		arraySchemaEvolution.context.tiledbContext.Get(),
		arraySchemaEvolution.tiledbArraySchemaEvolution.Get(),
		C.tiledb_serialization_type_t(serializationType),
		cClientSide, &bufferPtr)
	runtime.KeepAlive(arraySchemaEvolution)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("error serializing array schem evolution: %w",
			arraySchemaEvolution.context.LastError())
	}

	return newBufferFromHandle(arraySchemaEvolution.context, newBufferHandle(bufferPtr)), nil
}

// SerializeArraySchemaEvolution serializes the given array schema evolution.
//
// Deprecated: Use SerializeArraySchemaEvolutionToBuffer instead.
func SerializeArraySchemaEvolution(arraySchemaEvolution *ArraySchemaEvolution, serializationType SerializationType, clientSide bool) ([]byte, error) {
	buffer, err := SerializeArraySchemaEvolutionToBuffer(arraySchemaEvolution, serializationType, clientSide)
	if err != nil {
		return nil, err
	}

	return buffer.Serialize(serializationType)
}

// DeserializeArraySchemaEvolution deserializes a new array schema evolution object from the given buffer.
func DeserializeArraySchemaEvolution(buffer *Buffer, serializationType SerializationType, clientSide bool) (*ArraySchemaEvolution, error) {
	var cClientSide C.int32_t
	if clientSide {
		cClientSide = 1
	} else {
		cClientSide = 0
	}

	var arraySchemaEvolutionPtr *C.tiledb_array_schema_evolution_t
	ret := C.tiledb_deserialize_array_schema_evolution(
		buffer.context.tiledbContext.Get(), buffer.tiledbBuffer.Get(),
		C.tiledb_serialization_type_t(serializationType),
		cClientSide, &arraySchemaEvolutionPtr)
	runtime.KeepAlive(buffer)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("error deserializing array schema evolution: %w", buffer.context.LastError())
	}

	return newArraySchemaEvolutionFromHandle(buffer.context, newArraySchemaEvolutionHandle(arraySchemaEvolutionPtr)), nil
}

// SerializeArrayNonEmptyDomainToBuffer gets and serializes the array nonempty domain and returns a Buffer object containing the payload.
func SerializeArrayNonEmptyDomainToBuffer(a *Array, serializationType SerializationType) (*Buffer, error) {
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
	ret := C.tiledb_array_get_non_empty_domain(a.context.tiledbContext.Get(), a.tiledbArray.Get(), slicePtr(tmpDomain), &isEmpty)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("error serializing array nonempty domain: %w", a.context.LastError())
	}

	var cClientSide = C.int32_t(0) // Currently this parameter is unused in libtiledb
	var bufferPtr *C.tiledb_buffer_t
	ret = C.tiledb_serialize_array_nonempty_domain(a.context.tiledbContext.Get(), a.tiledbArray.Get(), slicePtr(tmpDomain), isEmpty, C.tiledb_serialization_type_t(serializationType), cClientSide, &bufferPtr)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("error serializing array nonempty domain: %w", a.context.LastError())
	}

	runtime.KeepAlive(a)
	return newBufferFromHandle(a.context, newBufferHandle(bufferPtr)), nil
}

// SerializeArrayNonEmptyDomain gets and serializes the array nonempty domain.
//
// Deprecated: Use SerializeArrayNonEmptyDomainToBuffer instead.
func SerializeArrayNonEmptyDomain(a *Array, serializationType SerializationType) ([]byte, error) {
	buffer, err := SerializeArrayNonEmptyDomainToBuffer(a, serializationType)
	if err != nil {
		return nil, err
	}

	return buffer.Serialize(serializationType)
}

// DeserializeArrayNonEmptyDomain deserializes an array nonempty domain.
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
	ret := C.tiledb_deserialize_array_nonempty_domain(a.context.tiledbContext.Get(), a.tiledbArray.Get(), buffer.tiledbBuffer.Get(), C.tiledb_serialization_type_t(serializationType), cClientSide, tmpDomainPtr, &isEmpty)
	runtime.KeepAlive(a)
	runtime.KeepAlive(buffer)
	if ret != C.TILEDB_OK {
		return nil, false, fmt.Errorf("error serializing array nonempty domain: %w", a.context.LastError())
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

// SerializeArrayNonEmptyDomainAllDimensionsToBuffer gets and serializes the array nonempty domain and returns a Buffer object containing the payload.
func SerializeArrayNonEmptyDomainAllDimensionsToBuffer(a *Array, serializationType SerializationType) (*Buffer, error) {
	var cClientSide = C.int32_t(0) // Currently this parameter is unused in libtiledb
	var bufferPtr *C.tiledb_buffer_t
	ret := C.tiledb_serialize_array_non_empty_domain_all_dimensions(a.context.tiledbContext.Get(), a.tiledbArray.Get(), C.tiledb_serialization_type_t(serializationType), cClientSide, &bufferPtr)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("error serializing array nonempty domain: %w", a.context.LastError())
	}

	return newBufferFromHandle(a.context, newBufferHandle(bufferPtr)), nil
}

// SerializeArrayNonEmptyDomainAllDimensions gets and serializes the array nonempty domain.
//
// Deprecated: Use SerializeArrayNonEmptyDomainAllDimensionsToBuffer instead.
func SerializeArrayNonEmptyDomainAllDimensions(a *Array, serializationType SerializationType) ([]byte, error) {
	buffer, err := SerializeArrayNonEmptyDomainAllDimensionsToBuffer(a, serializationType)
	if err != nil {
		return nil, err
	}

	return buffer.Serialize(serializationType)
}

// DeserializeArrayNonEmptyDomainAllDimensions deserializes an array nonempty domain.
func DeserializeArrayNonEmptyDomainAllDimensions(a *Array, buffer *Buffer, serializationType SerializationType) error {

	var cClientSide = C.int32_t(0) // Currently this parameter is unused in libtiledb
	ret := C.tiledb_deserialize_array_non_empty_domain_all_dimensions(a.context.tiledbContext.Get(), a.tiledbArray.Get(), buffer.tiledbBuffer.Get(), C.tiledb_serialization_type_t(serializationType), cClientSide)
	runtime.KeepAlive(a)
	runtime.KeepAlive(buffer)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("error deserializing array nonempty domain: %w", a.context.LastError())
	}

	return nil
}

// SerializeQuery serializes a query.
func SerializeQuery(query *Query, serializationType SerializationType, clientSide bool) (*BufferList, error) {
	var cClientSide C.int32_t
	if clientSide {
		cClientSide = 1
	} else {
		cClientSide = 0
	}

	var bufferListPtr *C.tiledb_buffer_list_t
	ret := C.tiledb_serialize_query(query.context.tiledbContext.Get(), query.tiledbQuery, C.tiledb_serialization_type_t(serializationType), cClientSide, &bufferListPtr)
	runtime.KeepAlive(query)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("error serializing query: %w", query.context.LastError())
	}

	return newBufferListFromHandle(query.context, newBufferListHandle(bufferListPtr)), nil
}

// DeserializeQuery deserializes a buffer into an existing query.
func DeserializeQuery(query *Query, buffer *Buffer, serializationType SerializationType, clientSide bool) error {
	var cClientSide C.int32_t
	if clientSide {
		cClientSide = 1
	} else {
		cClientSide = 0
	}

	ret := C.tiledb_deserialize_query(query.context.tiledbContext.Get(), buffer.tiledbBuffer.Get(), C.tiledb_serialization_type_t(serializationType), cClientSide, query.tiledbQuery)
	runtime.KeepAlive(query)
	runtime.KeepAlive(buffer)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("error deserializing query: %w", query.context.LastError())
	}

	return nil
}

// SerializeArrayMetadataToBuffer gets and serializes the array metadata and returns a Buffer object containing the payload.
func SerializeArrayMetadataToBuffer(a *Array, serializationType SerializationType) (*Buffer, error) {
	var bufferPtr *C.tiledb_buffer_t
	ret := C.tiledb_serialize_array_metadata(a.context.tiledbContext.Get(), a.tiledbArray.Get(), C.tiledb_serialization_type_t(serializationType), &bufferPtr)
	runtime.KeepAlive(a)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("error serializing array metadata: %w", a.context.LastError())
	}

	return newBufferFromHandle(a.context, newBufferHandle(bufferPtr)), nil
}

// SerializeArrayMetadata gets and serializes the array metadata.
//
// Deprecated: Use SerializeArrayMetadataToBuffer instead.
func SerializeArrayMetadata(a *Array, serializationType SerializationType) ([]byte, error) {
	buffer, err := SerializeArrayMetadataToBuffer(a, serializationType)
	if err != nil {
		return nil, err
	}

	return buffer.Serialize(serializationType)
}

// DeserializeArrayMetadata deserializes array metadata.
func DeserializeArrayMetadata(a *Array, buffer *Buffer, serializationType SerializationType) error {
	ret := C.tiledb_deserialize_array_metadata(a.context.tiledbContext.Get(), a.tiledbArray.Get(), C.tiledb_serialization_type_t(serializationType), buffer.tiledbBuffer.Get())
	runtime.KeepAlive(a)
	runtime.KeepAlive(buffer)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("error deserializing array metadata: %w", a.context.LastError())
	}
	return nil
}

// SerializeQueryEstResultSizesToBuffer gets and serializes the query estimated result sizes and returns a Buffer object containing the payload.
func SerializeQueryEstResultSizesToBuffer(q *Query, serializationType SerializationType, clientSide bool) (*Buffer, error) {
	var cClientSide C.int32_t
	if clientSide {
		cClientSide = 1
	} else {
		cClientSide = 0
	}

	var bufferPtr *C.tiledb_buffer_t
	ret := C.tiledb_serialize_query_est_result_sizes(q.context.tiledbContext.Get(), q.tiledbQuery, C.tiledb_serialization_type_t(serializationType), cClientSide, &bufferPtr)
	runtime.KeepAlive(q)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("error serializing query est buffer sizes: %w", q.context.LastError())
	}

	return newBufferFromHandle(q.context, newBufferHandle(bufferPtr)), nil
}

// SerializeQueryEstResultSizes gets and serializes the query estimated result sizes.
//
// Deprecated: Use SerializeQueryEstResultSizesToBuffer instead.
func SerializeQueryEstResultSizes(q *Query, serializationType SerializationType, clientSide bool) ([]byte, error) {
	buffer, err := SerializeQueryEstResultSizesToBuffer(q, serializationType, clientSide)
	if err != nil {
		return nil, err
	}

	return buffer.Serialize(serializationType)
}

// DeserializeQueryEstResultSizes deserializes query estimated result sizes.
func DeserializeQueryEstResultSizes(q *Query, buffer *Buffer, serializationType SerializationType, clientSide bool) error {
	var cClientSide C.int32_t
	if clientSide {
		cClientSide = 1
	} else {
		cClientSide = 0
	}

	ret := C.tiledb_deserialize_query_est_result_sizes(q.context.tiledbContext.Get(), q.tiledbQuery, C.tiledb_serialization_type_t(serializationType), cClientSide, buffer.tiledbBuffer.Get())
	runtime.KeepAlive(q)
	runtime.KeepAlive(buffer)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("error deserializing query est buffer sizes: %w", q.context.LastError())
	}
	return nil
}

// SerializeArrayToBuffer serializes an array and returns a Buffer object containing the payload.
func SerializeArrayToBuffer(array *Array, serializationType SerializationType, clientSide bool) (*Buffer, error) {
	var cClientSide C.int32_t
	if clientSide {
		cClientSide = 1
	} else {
		cClientSide = 0
	}

	var bufferPtr *C.tiledb_buffer_t
	ret := C.tiledb_serialize_array(array.context.tiledbContext.Get(), array.tiledbArray.Get(), C.tiledb_serialization_type_t(serializationType), cClientSide, &bufferPtr)
	runtime.KeepAlive(array)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("error serializing array: %w", array.context.LastError())
	}

	return newBufferFromHandle(array.context, newBufferHandle(bufferPtr)), nil
}

// SerializeArray serializes an array.
//
// Deprecated: Use SerializeArrayToBuffer instead.
func SerializeArray(array *Array, serializationType SerializationType, clientSide bool) ([]byte, error) {
	buffer, err := SerializeArrayToBuffer(array, serializationType, clientSide)
	if err != nil {
		return nil, err
	}

	return buffer.Serialize(serializationType)
}

// DeserializeArray deserializes a new array from the given buffer.
func DeserializeArray(buffer *Buffer, serializationType SerializationType, clientSide bool, arrayURI string) (*Array, error) {
	var cClientSide C.int32_t
	if clientSide {
		cClientSide = 1
	} else {
		cClientSide = 0
	}

	cArrayURI := C.CString(arrayURI)
	defer C.free(unsafe.Pointer(cArrayURI))

	var arrayPtr *C.tiledb_array_t
	ret := C.tiledb_deserialize_array(buffer.context.tiledbContext.Get(), buffer.tiledbBuffer.Get(), C.tiledb_serialization_type_t(serializationType), cClientSide, cArrayURI, &arrayPtr)
	runtime.KeepAlive(buffer)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("error deserializing array: %w", buffer.context.LastError())
	}

	return newArrayFromHandle(buffer.context, newArrayHandle(arrayPtr)), nil
}

// SerializeFragmentInfoToBuffer serializes fragment info and returns a Buffer object containing the payload.
func SerializeFragmentInfoToBuffer(fragmentInfo *FragmentInfo, serializationType SerializationType, clientSide bool) (*Buffer, error) {
	var cClientSide C.int32_t
	if clientSide {
		cClientSide = 1
	} else {
		cClientSide = 0
	}

	var bufferPtr *C.tiledb_buffer_t
	ret := C.tiledb_serialize_fragment_info(fragmentInfo.context.tiledbContext.Get(), fragmentInfo.tiledbFragmentInfo, C.tiledb_serialization_type_t(serializationType), cClientSide, &bufferPtr)
	runtime.KeepAlive(fragmentInfo)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("error serializing array: %w", fragmentInfo.context.LastError())
	}

	return newBufferFromHandle(fragmentInfo.context, newBufferHandle(bufferPtr)), nil
}

// SerializeFragmentInfo serializes fragment info.
//
// Deprecated: Use SerializeFragmentInfoToBuffer instead.
func SerializeFragmentInfo(fragmentInfo *FragmentInfo, serializationType SerializationType, clientSide bool) ([]byte, error) {
	buffer, err := SerializeFragmentInfoToBuffer(fragmentInfo, serializationType, clientSide)
	if err != nil {
		return nil, err
	}

	return buffer.Serialize(serializationType)
}

// DeserializeFragmentInfo deserializes an existing fragment info from the given buffer.
func DeserializeFragmentInfo(fragmentInfo FragmentInfo, buffer *Buffer, arrayURI string, serializationType SerializationType, clientSide bool) error {
	var cClientSide C.int32_t
	if clientSide {
		cClientSide = 1
	} else {
		cClientSide = 0
	}

	cArrayURI := C.CString(arrayURI)
	defer C.free(unsafe.Pointer(cArrayURI))

	ret := C.tiledb_deserialize_fragment_info(fragmentInfo.context.tiledbContext.Get(), buffer.tiledbBuffer.Get(), C.tiledb_serialization_type_t(serializationType), cArrayURI, cClientSide, fragmentInfo.tiledbFragmentInfo)
	runtime.KeepAlive(fragmentInfo)
	runtime.KeepAlive(buffer)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("error deserializing array: %w", fragmentInfo.context.LastError())
	}

	return nil
}

// SerializeFragmentInfoRequestToBuffer serializes fragment info and returns a Buffer object containing the payload.
func SerializeFragmentInfoRequestToBuffer(fragmentInfo *FragmentInfo, serializationType SerializationType, clientSide bool) (*Buffer, error) {
	var cClientSide C.int32_t
	if clientSide {
		cClientSide = 1
	} else {
		cClientSide = 0
	}

	var bufferPtr *C.tiledb_buffer_t
	ret := C.tiledb_serialize_fragment_info_request(fragmentInfo.context.tiledbContext.Get(), fragmentInfo.tiledbFragmentInfo, C.tiledb_serialization_type_t(serializationType), cClientSide, &bufferPtr)
	runtime.KeepAlive(fragmentInfo)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("error serializing array: %w", fragmentInfo.context.LastError())
	}

	return newBufferFromHandle(fragmentInfo.context, newBufferHandle(bufferPtr)), nil
}

// SerializeFragmentInfoRequest serializes fragment info.
//
// Deprecated: Use SerializeFragmentInfoRequestToBuffer instead.
func SerializeFragmentInfoRequest(fragmentInfo *FragmentInfo, serializationType SerializationType, clientSide bool) ([]byte, error) {
	buffer, err := SerializeFragmentInfoRequestToBuffer(fragmentInfo, serializationType, clientSide)
	if err != nil {
		return nil, err
	}

	return buffer.Serialize(serializationType)
}

// DeserializeFragmentInfoRequest deserializes an existing fragment info from the given buffer.
func DeserializeFragmentInfoRequest(fragmentInfo FragmentInfo, buffer *Buffer, serializationType SerializationType, clientSide bool) error {
	var cClientSide C.int32_t
	if clientSide {
		cClientSide = 1
	} else {
		cClientSide = 0
	}

	ret := C.tiledb_deserialize_fragment_info_request(fragmentInfo.context.tiledbContext.Get(), buffer.tiledbBuffer.Get(), C.tiledb_serialization_type_t(serializationType), cClientSide, fragmentInfo.tiledbFragmentInfo)
	runtime.KeepAlive(fragmentInfo)
	runtime.KeepAlive(buffer)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("error deserializing array: %w", fragmentInfo.context.LastError())
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

	query := &Query{
		context: context,
	}

	var arrayPtr *C.tiledb_array_t
	ret := C.tiledb_deserialize_query_and_array(context.tiledbContext.Get(), buffer.tiledbBuffer.Get(), C.tiledb_serialization_type_t(serializationType), cClientSide, cArrayURI, &query.tiledbQuery, &arrayPtr)
	if ret != C.TILEDB_OK {
		return nil, nil, fmt.Errorf("error deserializing query: %w", context.LastError())
	}

	array := newArrayFromHandle(context, newArrayHandle(arrayPtr))
	query.array = array
	freeOnGC(query)

	query.resultBufferElements = make(map[string][3]*uint64)

	// Make sure the buffer stays alive untill after the deserialization is complete
	runtime.KeepAlive(buffer)
	return array, query, nil
}

// SerializeGroupMetadata gets and serializes the group metadata and returns a Buffer object containing the payload
func SerializeGroupMetadataToBuffer(g *Group, serializationType SerializationType) (*Buffer, error) {
	var bufferPtr *C.tiledb_buffer_t
	ret := C.tiledb_serialize_group_metadata(g.context.tiledbContext.Get(), g.group, C.tiledb_serialization_type_t(serializationType), &bufferPtr)
	runtime.KeepAlive(g)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("error serializing group metadata: %w", g.context.LastError())
	}

	return newBufferFromHandle(g.context, newBufferHandle(bufferPtr)), nil
}

// SerializeGroupMetadata gets and serializes the group metadata
//
// Deprecated: Use SerializeGroupMetadataToBuffer instead.
func SerializeGroupMetadata(g *Group, serializationType SerializationType) ([]byte, error) {
	buffer, err := SerializeGroupMetadataToBuffer(g, serializationType)
	if err != nil {
		return nil, err
	}

	return buffer.Serialize(serializationType)
}

// DeserializeGroupMetadata deserializes group metadata
func DeserializeGroupMetadata(g *Group, buffer *Buffer, serializationType SerializationType) error {
	b, err := buffer.dataCopy()
	if err != nil {
		return errors.New("failed to retrieve bytes from buffer")
	}
	// cstrings are null terminated. Go's are not, add it as a suffix
	if err := buffer.SetBuffer(append(b, []byte("\u0000")...)); err != nil {
		return errors.New("failed to add null terminator to buffer")
	}

	ret := C.tiledb_deserialize_group_metadata(g.context.tiledbContext.Get(), g.group, C.tiledb_serialization_type_t(serializationType), buffer.tiledbBuffer.Get())
	runtime.KeepAlive(g)
	runtime.KeepAlive(buffer)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("error deserializing group metadata: %w", g.context.LastError())
	}

	return nil
}

// Deserialize deserializes the group from the given buffer.
func (g *Group) Deserialize(buffer *Buffer, serializationType SerializationType, clientSide bool) error {
	var cClientSide C.int32_t
	if clientSide {
		cClientSide = 1
	} else {
		cClientSide = 0
	}

	b, err := buffer.dataCopy()
	if err != nil {
		return errors.New("failed to retrieve bytes from buffer")
	}

	// cstrings are null terminated. Go's are not, add it as a suffix
	if err := buffer.SetBuffer(append(b, []byte("\u0000")...)); err != nil {
		return errors.New("failed to add null terminator to buffer")
	}

	ret := C.tiledb_deserialize_group(g.context.tiledbContext.Get(), buffer.tiledbBuffer.Get(), C.tiledb_serialization_type_t(serializationType), cClientSide, g.group)
	runtime.KeepAlive(g)
	runtime.KeepAlive(buffer)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("error deserializing group: %w", g.context.LastError())
	}

	return nil
}

// HandleLoadArraySchemaRequest Passes the array and serialized LoadArraySchemaRequest to core which returns the
// serialized LoadArraySchemaResponse. The request contains a TileDB Config used to load the schema, the response
// contains the latest array schema loaded and a map of all array schemas.
func HandleLoadArraySchemaRequest(array *Array, request *Buffer, serializationType SerializationType) (*Buffer, error) {
	response, err := NewBuffer(array.context)
	if err != nil {
		return nil, fmt.Errorf("error creating LoadArraySchemaResponse buffer: %w", array.context.LastError())
	}

	ret := C.tiledb_handle_load_array_schema_request(array.context.tiledbContext.Get(), array.tiledbArray.Get(),
		C.tiledb_serialization_type_t(serializationType), request.tiledbBuffer.Get(), response.tiledbBuffer.Get())
	runtime.KeepAlive(array)
	runtime.KeepAlive(request)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("error handling LoadArraySchemaRequset: %w", array.context.LastError())
	}

	return response, nil
}

// HandleArrayDeleteFragmentsTimestampsRequest is used by TileDB cloud to handle DeleteFragments with tiledb:// uris.
func HandleArrayDeleteFragmentsTimestampsRequest(context *Context, array *Array, buffer *Buffer, serializationType SerializationType) error {
	ret := C.tiledb_handle_array_delete_fragments_timestamps_request(context.tiledbContext.Get(), array.tiledbArray.Get(),
		C.tiledb_serialization_type_t(serializationType), buffer.tiledbBuffer.Get())
	runtime.KeepAlive(array)
	runtime.KeepAlive(buffer)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("error deserializing delete fragments timestamps: %w", context.LastError())
	}

	return nil
}

// HandleArrayDeleteFragmentsListRequest is used by TileDB cloud to handle DeleteFragmentsList with tiledb:// uris.
func HandleArrayDeleteFragmentsListRequest(context *Context, array *Array, buffer *Buffer, serializationType SerializationType) error {
	ret := C.tiledb_handle_array_delete_fragments_list_request(context.tiledbContext.Get(), array.tiledbArray.Get(),
		C.tiledb_serialization_type_t(serializationType), buffer.tiledbBuffer.Get())
	runtime.KeepAlive(array)
	runtime.KeepAlive(buffer)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("error deserializing delete fragments list: %w", context.LastError())
	}

	return nil
}

// HandleQueryPlanRequest handles a request for a query plan. This is used by TileDB-Cloud
// It returns a buffer with the serialized response. The caller should free the buffer after use.
func HandleQueryPlanRequest(array *Array, serializationType SerializationType, request *Buffer) (*Buffer, error) {
	opContext := array.context

	response, err := NewBuffer(opContext)
	if err != nil {
		return nil, fmt.Errorf("error allocating tiledb buffer: %w", opContext.LastError())
	}

	ret := C.tiledb_handle_query_plan_request(opContext.tiledbContext.Get(), array.tiledbArray.Get(), C.tiledb_serialization_type_t(serializationType),
		request.tiledbBuffer.Get(), response.tiledbBuffer.Get())
	runtime.KeepAlive(array)
	runtime.KeepAlive(request)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("error handling query plan request: %w", opContext.LastError())
	}

	return response, nil
}

// HandleConsolidationPlanRequest handles a request for a consolidation plan. This is used by TileDB-Cloud
// It returns a buffer with the serialized response. The caller should free the buffer after use.
func HandleConsolidationPlanRequest(array *Array, serializationType SerializationType, request *Buffer) (*Buffer, error) {
	opContext := array.context

	response, err := NewBuffer(opContext)
	if err != nil {
		return nil, fmt.Errorf("error allocating tiledb buffer: %w", opContext.LastError())
	}

	ret := C.tiledb_handle_consolidation_plan_request(opContext.tiledbContext.Get(), array.tiledbArray.Get(), C.tiledb_serialization_type_t(serializationType),
		request.tiledbBuffer.Get(), response.tiledbBuffer.Get())
	runtime.KeepAlive(array)
	runtime.KeepAlive(request)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("error handling consolidation plan request: %w", opContext.LastError())
	}

	return response, nil
}

// DeserializeLoadEnumerationsRequest deserializes a LoadEnumerationsRequests. This is used by TileDB-Cloud.
func DeserializeLoadEnumerationsRequest(array *Array, serializationType SerializationType, request *Buffer) (*Buffer, error) {
	response, err := NewBuffer(array.context)
	if err != nil {
		return nil, fmt.Errorf("error deserializing load enumerations request: %w", array.context.LastError())
	}

	ret := C.tiledb_handle_load_enumerations_request(array.context.tiledbContext.Get(), array.tiledbArray.Get(), C.tiledb_serialization_type_t(serializationType),
		request.tiledbBuffer.Get(), response.tiledbBuffer.Get())
	runtime.KeepAlive(array)
	runtime.KeepAlive(request)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("error deserializing load enumerations request: %w", array.context.LastError())
	}

	return response, nil
}
