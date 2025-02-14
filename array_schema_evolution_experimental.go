package tiledb

/*
#include <tiledb/tiledb_experimental.h>
#include <stdlib.h>
*/
import "C"

import (
	"errors"
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
	runtime.KeepAlive(tdbCtx)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("error creating tiledb arraySchemaEvolution: %w",
			arraySchemaEvolution.context.LastError())
	}
	freeOnGC(&arraySchemaEvolution)

	return &arraySchemaEvolution, nil
}

// Free releases the internal TileDB core data that was allocated on the C heap.
// It is automatically called when this object is garbage collected, but can be
// called earlier to manually release memory if needed. Free is idempotent and
// can safely be called many times on the same object; if it has already
// been freed, it will not be freed again.
func (ase *ArraySchemaEvolution) Free() {
	if ase.tiledbArraySchemaEvolution != nil {
		C.tiledb_array_schema_evolution_free(&ase.tiledbArraySchemaEvolution)
	}
}

// Context exposes the internal TileDB context used to initialize the array schema evolution
func (ase *ArraySchemaEvolution) Context() *Context {
	return ase.context
}

// AddAttribute adds an attribute to an array schema evolution.
func (ase *ArraySchemaEvolution) AddAttribute(attribute *Attribute) error {
	name, err := attribute.Name()
	if err != nil {
		return errors.New("cannot get name from attribute")
	}

	ret := C.tiledb_array_schema_evolution_add_attribute(
		ase.context.tiledbContext, ase.tiledbArraySchemaEvolution,
		attribute.tiledbAttribute)
	runtime.KeepAlive(ase)
	runtime.KeepAlive(attribute)
	if ret != C.TILEDB_OK {
		return fmt.Errorf(
			"error adding attribute %s to tiledb arraySchemaEvolution: %w",
			name, ase.context.LastError())
	}

	return nil
}

// DropAttribute drops an attribute to an array schema evolution.
func (ase *ArraySchemaEvolution) DropAttribute(name string) error {
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))

	ret := C.tiledb_array_schema_evolution_drop_attribute(
		ase.context.tiledbContext, ase.tiledbArraySchemaEvolution, cname)
	runtime.KeepAlive(ase)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("error dropping tiledb attribute: %w",
			ase.context.LastError())
	}

	return nil
}

// Evolve evolves array schema of an array.
func (ase *ArraySchemaEvolution) Evolve(uri string) error {
	curi := C.CString(uri)
	defer C.free(unsafe.Pointer(curi))

	ret := C.tiledb_array_evolve(ase.context.tiledbContext, curi,
		ase.tiledbArraySchemaEvolution)
	runtime.KeepAlive(ase)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("error evolving schema for array %s: %w", uri,
			ase.context.LastError())
	}

	return nil
}
