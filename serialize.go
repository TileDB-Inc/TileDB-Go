package tiledb

/*
#cgo LDFLAGS: -ltiledb
#cgo linux LDFLAGS: -ldl
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
func SerializeArraySchema(schema *ArraySchema, serializationType SerializationType, clientSide bool) (*Buffer, error) {
	var cClientSide C.int32_t
	if clientSide {
		cClientSide = 1
	} else {
		cClientSide = 0
	}

	buffer := Buffer{context: schema.context}
	// Set finalizer for free C pointer on gc
	runtime.SetFinalizer(&buffer, func(buffer *Buffer) {
		buffer.Free()
	})

	ret := C.tiledb_serialize_array_schema(schema.context.tiledbContext, schema.tiledbArraySchema, C.tiledb_serialization_type_t(serializationType), cClientSide, &buffer.tiledbBuffer)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error serializing array schema: %s", schema.context.LastError())
	}

	return &buffer, nil
}

// DeserializeArraySchema deserializes a new array schema from the given buffer
func DeserializeArraySchema(buffer *Buffer, serializationType SerializationType, clientSide bool) (*ArraySchema, error) {
	schema := ArraySchema{context: buffer.context}
	// Set finalizer for free C pointer on gc
	runtime.SetFinalizer(&schema, func(arraySchema *ArraySchema) {
		arraySchema.Free()
	})

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

	return &schema, nil
}

// SerializeArrayNonEmptyDomain gets and serializes the array nonempty domain
func SerializeArrayNonEmptyDomain(a *Array, serializationType SerializationType) (*Buffer, error) {
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
	// Set finalizer for free C pointer on gc
	runtime.SetFinalizer(&buffer, func(buffer *Buffer) {
		buffer.Free()
	})

	var cClientSide = C.int32_t(0) // Currently this parameter is unused in libtiledb
	ret = C.tiledb_serialize_array_nonempty_domain(a.context.tiledbContext, a.tiledbArray, unsafe.Pointer(&tmpDomain[0]), isEmpty, C.tiledb_serialization_type_t(serializationType), cClientSide, &buffer.tiledbBuffer)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error serializing array nonempty domain: %s", a.context.LastError())
	}

	return &buffer, nil
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
	} else {
		nonEmptyDomains, err := makeNonEmptyDomain(domain, tmpDomain)
		if err != nil {
			return nil, false, err
		}
		return nonEmptyDomains, false, nil
	}
}

// SerializeArrayNonEmptyDomainAllDimensions gets and serializes the array nonempty domain
func SerializeArrayNonEmptyDomainAllDimensions(a *Array, serializationType SerializationType) (*Buffer, error) {

	buffer := Buffer{context: a.context}
	// Set finalizer for free C pointer on gc
	runtime.SetFinalizer(&buffer, func(buffer *Buffer) {
		buffer.Free()
	})

	var cClientSide = C.int32_t(0) // Currently this parameter is unused in libtiledb
	ret := C.tiledb_serialize_array_non_empty_domain_all_dimensions(a.context.tiledbContext, a.tiledbArray, C.tiledb_serialization_type_t(serializationType), cClientSide, &buffer.tiledbBuffer)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error serializing array nonempty domain: %s", a.context.LastError())
	}

	return &buffer, nil
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
func SerializeArrayMaxBufferSizes(a *Array, subarray interface{}, serializationType SerializationType) (*Buffer, error) {
	// Create subarray void*
	var cSubarray unsafe.Pointer
	if reflect.TypeOf(subarray).Kind() != reflect.Slice {
		return nil, fmt.Errorf("subarray passed must be a slice, type passed was: %s", reflect.TypeOf(subarray).Kind().String())
	}
	subarrayType := reflect.TypeOf(subarray).Elem().Kind()
	switch subarrayType {
	case reflect.Int:
		tmpSubArray := subarray.([]int)
		cSubarray = unsafe.Pointer(&tmpSubArray[0])
	case reflect.Int8:
		tmpSubArray := subarray.([]int8)
		cSubarray = unsafe.Pointer(&tmpSubArray[0])
	case reflect.Int16:
		tmpSubArray := subarray.([]int16)
		cSubarray = unsafe.Pointer(&tmpSubArray[0])
	case reflect.Int32:
		tmpSubArray := subarray.([]int32)
		cSubarray = unsafe.Pointer(&tmpSubArray[0])
	case reflect.Int64:
		tmpSubArray := subarray.([]int64)
		cSubarray = unsafe.Pointer(&tmpSubArray[0])
	case reflect.Uint:
		tmpSubArray := subarray.([]uint)
		cSubarray = unsafe.Pointer(&tmpSubArray[0])
	case reflect.Uint8:
		tmpSubArray := subarray.([]uint8)
		cSubarray = unsafe.Pointer(&tmpSubArray[0])
	case reflect.Uint16:
		tmpSubArray := subarray.([]uint16)
		cSubarray = unsafe.Pointer(&tmpSubArray[0])
	case reflect.Uint32:
		tmpSubArray := subarray.([]uint32)
		cSubarray = unsafe.Pointer(&tmpSubArray[0])
	case reflect.Uint64:
		tmpSubArray := subarray.([]uint64)
		cSubarray = unsafe.Pointer(&tmpSubArray[0])
	case reflect.Float32:
		tmpSubArray := subarray.([]float32)
		cSubarray = unsafe.Pointer(&tmpSubArray[0])
	case reflect.Float64:
		tmpSubArray := subarray.([]float64)
		cSubarray = unsafe.Pointer(&tmpSubArray[0])
	default:
		return nil, fmt.Errorf("unhandled subarray datatype: %s", subarrayType.String())
	}

	buffer := Buffer{context: a.context}
	// Set finalizer for free C pointer on gc
	runtime.SetFinalizer(&buffer, func(buffer *Buffer) {
		buffer.Free()
	})

	ret := C.tiledb_serialize_array_max_buffer_sizes(a.context.tiledbContext, a.tiledbArray, cSubarray, C.tiledb_serialization_type_t(serializationType), &buffer.tiledbBuffer)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("error serializing array max buffer sizes: %s", a.context.LastError())
	}

	return &buffer, nil
}

// SerializeQuery serializes a query
func SerializeQuery(query *Query, serializationType SerializationType, clientSide bool) (*BufferList, error) {
	bufferList := BufferList{context: query.context}
	// Set finalizer for free C pointer on gc
	runtime.SetFinalizer(&bufferList, func(bufferList *BufferList) {
		bufferList.Free()
	})

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
func SerializeArrayMetadata(a *Array, serializationType SerializationType) (*Buffer, error) {
	buffer := Buffer{context: a.context}
	// Set finalizer for free C pointer on gc
	runtime.SetFinalizer(&buffer, func(buffer *Buffer) {
		buffer.Free()
	})

	ret := C.tiledb_serialize_array_metadata(a.context.tiledbContext, a.tiledbArray, C.tiledb_serialization_type_t(serializationType), &buffer.tiledbBuffer)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error serializing array metadata: %s", a.context.LastError())
	}

	return &buffer, nil
}

// DeserializeArrayMetadata deserializes array metadata
func DeserializeArrayMetadata(a *Array, buffer *Buffer, serializationType SerializationType) error {
	ret := C.tiledb_deserialize_array_metadata(a.context.tiledbContext, a.tiledbArray, C.tiledb_serialization_type_t(serializationType), buffer.tiledbBuffer)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error deserializing array metadata: %s", a.context.LastError())
	}
	return nil
}
