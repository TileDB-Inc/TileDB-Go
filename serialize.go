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

	buffer, err := NewBuffer(schema.context)
	if err != nil {
		return nil, fmt.Errorf("Error serializing array schema: %s", schema.context.LastError())
	}

	ret := C.tiledb_serialize_array_schema(schema.context.tiledbContext, schema.tiledbArraySchema, C.tiledb_serialization_type_t(serializationType), cClientSide, buffer.tiledbBuffer)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error serializing array schema: %s", schema.context.LastError())
	}

	return buffer, nil
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
	dataTypeSize := uint(C.tiledb_datatype_size(C.tiledb_datatype_t(domainType)))
	subarraySize := 2 * ndims * dataTypeSize

	var isEmpty C.int32_t
	tmpDomain := make([]uint8, subarraySize)
	ret := C.tiledb_array_get_non_empty_domain(a.context.tiledbContext, a.tiledbArray, unsafe.Pointer(&tmpDomain[0]), &isEmpty)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error serializing array nonempty domain: %s", a.context.LastError())
	}

	buffer, err := NewBuffer(a.context)
	if err != nil {
		return nil, err
	}

	var cClientSide = C.int32_t(0) // Currently this parameter is unused in libtiledb
	ret = C.tiledb_serialize_array_nonempty_domain(a.context.tiledbContext, a.tiledbArray, unsafe.Pointer(&tmpDomain[0]), isEmpty, C.tiledb_serialization_type_t(serializationType), cClientSide, buffer.tiledbBuffer)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error serializing array nonempty domain: %s", a.context.LastError())
	}

	return buffer, nil
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
