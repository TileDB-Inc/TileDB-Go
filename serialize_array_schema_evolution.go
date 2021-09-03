//go:build experimental
// +build experimental

// This file declares Go bindings for experimental features in TileDB.
// Experimental APIs to do not fall under the API compatibility guarantees and
// might change between TileDB versions

package tiledb

/*
#cgo LDFLAGS: -ltiledb
#cgo linux LDFLAGS: -ldl
#include <tiledb/tiledb_experimental.h>
#include <tiledb/tiledb_serialization.h>
#include <stdlib.h>
*/
import "C"

import (
	"fmt"
	"runtime"
)

// SerializeArraySchemaEvolution serializes the given array schema evolution
func SerializeArraySchemaEvolution(arraySchemaEvolution *ArraySchemaEvolution, serializationType SerializationType, clientSide bool) (*Buffer, error) {
	var cClientSide C.int32_t
	if clientSide {
		cClientSide = 1
	} else {
		cClientSide = 0
	}

	buffer := Buffer{context: arraySchemaEvolution.context}
	// Set finalizer for free C pointer on gc
	runtime.SetFinalizer(&buffer, func(buffer *Buffer) {
		buffer.Free()
	})

	ret := C.tiledb_serialize_array_schema_evolution(
		arraySchemaEvolution.context.tiledbContext,
		arraySchemaEvolution.tiledbArraySchemaEvolution,
		C.tiledb_serialization_type_t(serializationType),
		cClientSide, &buffer.tiledbBuffer)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error serializing array schem evolution: %s",
			arraySchemaEvolution.context.LastError())
	}

	return &buffer, nil
}

// DeserializeArraySchemaEvolution deserializes a new array schema evolution object from the given buffer
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

	// Set finalizer for free C pointer on gc
	// This needs to happen *after* the tiledb_deserialize_array_schema_evolution
	// call because that may leave the schemaEvolution with a non-nil pointer
	// to already-freed memory.
	runtime.SetFinalizer(&arraySchemaEvolution, func(arraySchemaEvolution *ArraySchemaEvolution) {
		arraySchemaEvolution.Free()
	})

	return &arraySchemaEvolution, nil
}
