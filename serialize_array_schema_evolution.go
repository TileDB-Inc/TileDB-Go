package tiledb

/*
#include <tiledb/tiledb_serialization.h>
#include <stdlib.h>
*/
import "C"

import (
	"fmt"
)

// SerializeArraySchemaEvolution serializes the given array schema evolution.
func SerializeArraySchemaEvolution(arraySchemaEvolution *ArraySchemaEvolution, serializationType SerializationType, clientSide bool) ([]byte, error) {
	var cClientSide C.int32_t
	if clientSide {
		cClientSide = 1
	} else {
		cClientSide = 0
	}

	buffer := Buffer{context: arraySchemaEvolution.context}
	freeOnGC(&buffer)

	ret := C.tiledb_serialize_array_schema_evolution(
		arraySchemaEvolution.context.tiledbContext,
		arraySchemaEvolution.tiledbArraySchemaEvolution,
		C.tiledb_serialization_type_t(serializationType),
		cClientSide, &buffer.tiledbBuffer)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error serializing array schem evolution: %s",
			arraySchemaEvolution.context.LastError())
	}

	return buffer.Serialize(serializationType)
}

// DeserializeArraySchemaEvolution deserializes a new array schema evolution object from the given buffer.
func DeserializeArraySchemaEvolution(buffer *Buffer, serializationType SerializationType, clientSide bool) (*ArraySchemaEvolution, error) {
	arraySchemaEvolution := ArraySchemaEvolution{context: buffer.context}

	var cClientSide C.int32_t
	if clientSide {
		cClientSide = 1
	} else {
		cClientSide = 0
	}

	ret := C.tiledb_deserialize_array_schema_evolution(
		arraySchemaEvolution.context.tiledbContext, buffer.tiledbBuffer,
		C.tiledb_serialization_type_t(serializationType),
		cClientSide, &arraySchemaEvolution.tiledbArraySchemaEvolution)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error deserializing array schema evolution: %s", arraySchemaEvolution.context.LastError())
	}

	// This needs to happen *after* the tiledb_deserialize_array_schema_evolution
	// call because that may leave the schemaEvolution with a non-nil pointer
	// to already-freed memory.
	freeOnGC(&arraySchemaEvolution)

	return &arraySchemaEvolution, nil
}
