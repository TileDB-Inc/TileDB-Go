package tiledb

/*
#cgo LDFLAGS: -ltiledb
#cgo linux LDFLAGS: -ldl
#include <tiledb/tiledb.h>
#include <stdio.h>
#include <stdlib.h>
*/
import "C"

import (
	"fmt"
	"runtime"
)

// SerializeArraySchema serializes an array schema
func SerializeArraySchema(schema *ArraySchema, serializationType SerializationType) (*Buffer, error) {
	buffer, err := NewBuffer(schema.context)
	if err != nil {
		return nil, fmt.Errorf("Error serializing array schema: %s", schema.context.LastError())
	}

	ret := C.tiledb_serialize_array_schema(schema.context.tiledbContext, schema.tiledbArraySchema, C.tiledb_serialization_type_t(serializationType), buffer.tiledbBuffer)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error serializing array schema: %s", schema.context.LastError())
	}

	return buffer, nil
}

// DeserializeArraySchema deserializes a new array schema from the given buffer
func DeserializeArraySchema(buffer *Buffer, serializationType SerializationType) (*ArraySchema, error) {
	schema := ArraySchema{context: buffer.context}
	// Set finalizer for free C pointer on gc
	runtime.SetFinalizer(&schema, func(arraySchema *ArraySchema) {
		arraySchema.Free()
	})

	ret := C.tiledb_deserialize_array_schema(schema.context.tiledbContext, &schema.tiledbArraySchema, C.tiledb_serialization_type_t(serializationType), buffer.tiledbBuffer)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error deserializing array schema: %s", schema.context.LastError())
	}

	return &schema, nil
}

// SerializeQuery serializes a query
func SerializeQuery(query *Query, serializationType SerializationType) (*Buffer, error) {
	buffer, err := NewBuffer(query.context)
	if err != nil {
		return nil, fmt.Errorf("Error serializing query: %s", query.context.LastError())
	}

	ret := C.tiledb_serialize_query(query.context.tiledbContext, query.tiledbQuery, C.tiledb_serialization_type_t(serializationType), buffer.tiledbBuffer)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error serializing query: %s", query.context.LastError())
	}

	return buffer, nil
}

// DeserializeQuery deserializes a buffer into an existing query
func DeserializeQuery(query *Query, buffer *Buffer, serializationType SerializationType) error {
	ret := C.tiledb_deserialize_query(query.context.tiledbContext, query.tiledbQuery, C.tiledb_serialization_type_t(serializationType), buffer.tiledbBuffer)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error deserializing query: %s", query.context.LastError())
	}

	return nil
}
