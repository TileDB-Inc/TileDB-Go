/**
 * @file   array_schema_experimental.go
 *
 * @section DESCRIPTION
 *
 * This file declares Go bindings for experimental features in TileDB.
 * Experimental APIs to do not fall under the API compatibility guarantees and
 * might change between TileDB versions
 */

package tiledb

/*
#cgo LDFLAGS: -ltiledb
#cgo linux LDFLAGS: -ldl
#include <tiledb/tiledb_experimental.h>
#include <stdlib.h>
*/
import "C"

import (
	"fmt"
	"runtime"
	"unsafe"
)

type ArraySchemaEvolution struct {
	tiledbArraySchemaEvolution *C.tiledb_array_schema_evolution_t
	context                    *Context
}

// NewArraySchemaEvolution creates a TileDB schema evolution object.
func NewArraySchemaEvolution(tdbCtx *Context) (*ArraySchemaEvolution, error) {
	arraySchemaEvolution := ArraySchemaEvolution{context: tdbCtx}
	ret := C.tiledb_array_schema_evolution_alloc(
		arraySchemaEvolution.context.tiledbContext,
		&arraySchemaEvolution.tiledbArraySchemaEvolution)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error creating tiledb arraySchemaEvolution: %s",
			arraySchemaEvolution.context.LastError())
	}

	// Set finalizer for free C pointer on gc
	runtime.SetFinalizer(&arraySchemaEvolution,
		func(arraySchemaEvolution *ArraySchemaEvolution) {
			arraySchemaEvolution.Free()
		})

	return &arraySchemaEvolution, nil
}

// Free destroys an array schema evolution, freeing associated memory
func (ase *ArraySchemaEvolution) Free() {
	if ase.tiledbArraySchemaEvolution != nil {
		C.tiledb_array_schema_evolution_free(&ase.tiledbArraySchemaEvolution)
	}
}

// AddAttribute adds an attribute to an array schema evolution.
func (ase *ArraySchemaEvolution) AddAttribute(attribute *Attribute) error {
	ret := C.tiledb_array_schema_evolution_add_attribute(
		ase.context.tiledbContext, ase.tiledbArraySchemaEvolution,
		attribute.tiledbAttribute)

	name, err := attribute.Name()
	if err != nil {
		return fmt.Errorf("Cannot get name from attribute: %s",
			ase.context.LastError())
	}

	if ret != C.TILEDB_OK {
		return fmt.Errorf(
			"Error adding attribute %s to tiledb arraySchemaEvolution: %s",
			name, ase.context.LastError())
	}

	return nil
}

// DropAttribute drops an attribute to an array schema evolution
func (ase *ArraySchemaEvolution) DropAttribute(name string) error {
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))

	ret := C.tiledb_array_schema_evolution_drop_attribute(
		ase.context.tiledbContext, ase.tiledbArraySchemaEvolution, cname)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("error dropping tiledb attribute: %s",
			ase.context.tiledbContext.LastError())
	}

	return nil
}

// Evolve evolves array schema of an array
func (ase *ArraySchemaEvolution) Evolve(uri string) error {
	curi := C.CString(uri)
	defer C.free(unsafe.Pointer(curi))

	ret := C.tiledb_array_evolve(ase.context.tiledbContext, curi,
		ase.tiledbArraySchemaEvolution)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("error evolving schema for array %s: %s", uri,
			ase.context.tiledbContext.LastError())
	}

	return nil
}
