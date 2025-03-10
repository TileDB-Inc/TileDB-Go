package tiledb

/*
#include <tiledb/tiledb.h>
#include <stdlib.h>
*/
import "C"

import (
	"fmt"
	"os"
	"runtime"
	"unsafe"
)

// Domain represents the domain of an array.
// A Domain defines the set of Dimension objects for a given array.
// The properties of a Domain derive from the underlying dimensions.
// A Domain is a component of an ArraySchema.
type Domain struct {
	tiledbDomain *C.tiledb_domain_t
	context      *Context
}

// NewDomain allocates a new domain.
func NewDomain(tdbCtx *Context) (*Domain, error) {
	domain := Domain{context: tdbCtx}
	ret := C.tiledb_domain_alloc(domain.context.tiledbContext, &domain.tiledbDomain)
	runtime.KeepAlive(tdbCtx)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("error creating tiledb domain: %w", domain.context.LastError())
	}
	runtime.AddCleanup(&domain, freeFreeable, Freeable(&domain))

	return &domain, nil
}

// Free releases the internal TileDB core data that was allocated on the C heap.
// It is automatically called when this object is garbage collected, but can be
// called earlier to manually release memory if needed. Free is idempotent and
// can safely be called many times on the same object; if it has already
// been freed, it will not be freed again.
func (d *Domain) Free() {
	if d.tiledbDomain != nil {
		C.tiledb_domain_free(&d.tiledbDomain)
	}
}

// Context exposes the internal TileDB context used to initialize the domain.
func (d *Domain) Context() *Context {
	return d.context
}

// Type returns a domain's type deduced from dimensions.
func (d *Domain) Type() (Datatype, error) {
	var datatype C.tiledb_datatype_t
	ret := C.tiledb_domain_get_type(d.context.tiledbContext, d.tiledbDomain, &datatype)
	runtime.KeepAlive(d)
	if ret != C.TILEDB_OK {
		return -1, fmt.Errorf("error getting tiledb domain type: %w", d.context.LastError())
	}
	return Datatype(datatype), nil
}

// NDim returns the number of dimensions.
func (d *Domain) NDim() (uint, error) {
	var ndim C.uint32_t
	ret := C.tiledb_domain_get_ndim(d.context.tiledbContext, d.tiledbDomain, &ndim)
	runtime.KeepAlive(d)
	if ret != C.TILEDB_OK {
		return 0, fmt.Errorf("error getting tiledb domain number of dimensions: %w", d.context.LastError())
	}
	return uint(ndim), nil
}

// DimensionFromIndex retrieves a dimension object from a domain by index.
func (d *Domain) DimensionFromIndex(index uint) (*Dimension, error) {
	var dim *C.tiledb_dimension_t
	ret := C.tiledb_domain_get_dimension_from_index(d.context.tiledbContext,
		d.tiledbDomain, C.uint32_t(index), &dim)
	runtime.KeepAlive(d)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("error getting tiledb dimension by index for domain: %w", d.context.LastError())
	}

	dimension := Dimension{tiledbDimension: dim, context: d.context}
	runtime.AddCleanup(&dimension, freeFreeable, Freeable(&dimension))

	return &dimension, nil
}

// DimensionFromName retrieves a dimension object from a domain by name (key).
func (d *Domain) DimensionFromName(name string) (*Dimension, error) {
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))
	var dim *C.tiledb_dimension_t
	ret := C.tiledb_domain_get_dimension_from_name(d.context.tiledbContext, d.tiledbDomain, cname, &dim)
	runtime.KeepAlive(d)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("error getting tiledb dimension by name for domain: %w", d.context.LastError())
	}
	dimension := Dimension{tiledbDimension: dim, context: d.context}
	runtime.AddCleanup(&dimension, freeFreeable, Freeable(&dimension))
	return &dimension, nil
}

// AddDimensions adds one or more dimensions to a domain.
func (d *Domain) AddDimensions(dimensions ...*Dimension) error {
	for _, dimension := range dimensions {
		ret := C.tiledb_domain_add_dimension(d.context.tiledbContext, d.tiledbDomain, dimension.tiledbDimension)
		runtime.KeepAlive(dimension)
		if ret != C.TILEDB_OK {
			return fmt.Errorf("error adding dimension to domain: %w", d.context.LastError())
		}
	}
	return nil
}

// HasDimension returns true if dimension `dimName` is part of the domain.
func (d *Domain) HasDimension(dimName string) (bool, error) {
	var hasDim C.int32_t
	cDimName := C.CString(dimName)
	defer C.free(unsafe.Pointer(cDimName))
	ret := C.tiledb_domain_has_dimension(d.context.tiledbContext, d.tiledbDomain, cDimName, &hasDim)
	runtime.KeepAlive(d)
	if ret != C.TILEDB_OK {
		return false, fmt.Errorf("error finding dimension %s in domain: %w", dimName, d.context.LastError())
	}

	if hasDim == 0 {
		return false, nil
	}

	return true, nil
}

// DumpSTDOUT dumps the domain in ASCII format to stdout.
func (d *Domain) DumpSTDOUT() error {
	ret := C.tiledb_domain_dump(d.context.tiledbContext, d.tiledbDomain, C.stdout)
	runtime.KeepAlive(d)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("error dumping domain to stdout: %w", d.context.LastError())
	}
	return nil
}

// Dump dumps the domain in ASCII format to the given path.
func (d *Domain) Dump(path string) error {

	if _, err := os.Stat(path); err == nil {
		return fmt.Errorf("error path already %s exists", path)
	}

	// Convert to char *
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))

	// Set mode as char*
	cMode := C.CString("w")
	defer C.free(unsafe.Pointer(cMode))

	// Open file to get FILE*
	cFile := C.fopen(cPath, cMode)
	defer C.fclose(cFile)

	// Dump domain to file
	ret := C.tiledb_domain_dump(d.context.tiledbContext, d.tiledbDomain, cFile)
	runtime.KeepAlive(d)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("error dumping domain to file %s: %w", path, d.context.LastError())
	}
	return nil
}
