package tiledb

/*
#cgo LDFLAGS: -ltiledb
#cgo linux LDFLAGS: -ldl
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

// Domain Represents the domain of an array.
// A Domain defines the set of Dimension objects for a given array.
// The properties of a Domain derive from the underlying dimensions.
// A Domain is a component of an ArraySchema.
type Domain struct {
	tiledbDomain *C.tiledb_domain_t
	context      *Context
}

// NewDomain alloc a new domainuration
func NewDomain(tdbCtx *Context) (*Domain, error) {
	domain := Domain{context: tdbCtx}
	ret := C.tiledb_domain_alloc(domain.context.tiledbContext, &domain.tiledbDomain)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error creating tiledb domain: %s", domain.context.LastError())
	}

	// Set finalizer for free C pointer on gc
	runtime.SetFinalizer(&domain, func(domain *Domain) {
		domain.Free()
	})

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

// Type returns a domains type deduced from dimensions
func (d *Domain) Type() (Datatype, error) {
	var datatype C.tiledb_datatype_t
	ret := C.tiledb_domain_get_type(d.context.tiledbContext, d.tiledbDomain, &datatype)
	if ret != C.TILEDB_OK {
		return -1, fmt.Errorf("Error getting tiledb domain type: %s", d.context.LastError())
	}
	return Datatype(datatype), nil
}

// NDim returns the number of dimensions
func (d *Domain) NDim() (uint, error) {
	var ndim C.uint32_t
	ret := C.tiledb_domain_get_ndim(d.context.tiledbContext, d.tiledbDomain, &ndim)
	if ret != C.TILEDB_OK {
		return 0, fmt.Errorf("Error getting tiledb domain number of dimensions: %s", d.context.LastError())
	}
	return uint(ndim), nil
}

// DimensionFromIndex retrieves a dimension object from a domain by index.
func (d *Domain) DimensionFromIndex(index uint) (*Dimension, error) {
	var dim *C.tiledb_dimension_t
	ret := C.tiledb_domain_get_dimension_from_index(d.context.tiledbContext,
		d.tiledbDomain, C.uint32_t(index), &dim)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error getting tiledb dimension by index for domain: %s", d.context.LastError())
	}

	dimension := Dimension{tiledbDimension: dim, context: d.context}
	// Set finalizer for free C pointer on gc
	runtime.SetFinalizer(&dimension, func(dimension *Dimension) {
		dimension.Free()
	})

	return &dimension, nil
}

// DimensionFromName retrieves a dimension object from a domain by name (key)
func (d *Domain) DimensionFromName(name string) (*Dimension, error) {
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))
	var dim *C.tiledb_dimension_t
	ret := C.tiledb_domain_get_dimension_from_name(d.context.tiledbContext, d.tiledbDomain, cname, &dim)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error getting tiledb dimension by name for domain: %s", d.context.LastError())
	}
	dimension := Dimension{tiledbDimension: dim, context: d.context}
	// Set finalizer for free C pointer on gc
	runtime.SetFinalizer(&dimension, func(dimension *Dimension) {
		dimension.Free()
	})
	return &dimension, nil
}

// AddDimensions adds one or more dimensions to a domain
func (d *Domain) AddDimensions(dimensions ...*Dimension) error {
	for _, dimension := range dimensions {
		ret := C.tiledb_domain_add_dimension(d.context.tiledbContext, d.tiledbDomain, dimension.tiledbDimension)
		if ret != C.TILEDB_OK {
			return fmt.Errorf("Error adding dimension to domain: %s", d.context.LastError())
		}
	}
	return nil
}

// HasDimension returns true if dimension: `dimName` is part of the domain
func (d *Domain) HasDimension(dimName string) (bool, error) {
	var hasDim C.int32_t
	cDimName := C.CString(dimName)
	defer C.free(unsafe.Pointer(cDimName))
	ret := C.tiledb_domain_has_dimension(d.context.tiledbContext, d.tiledbDomain, cDimName, &hasDim)
	if ret != C.TILEDB_OK {
		return false, fmt.Errorf("Error finding dimension %s in domain: %s", dimName, d.context.LastError())
	}

	if hasDim == 0 {
		return false, nil
	}

	return true, nil
}

// DumpSTDOUT Dumps the domain in ASCII format to stdout
func (d *Domain) DumpSTDOUT() error {
	ret := C.tiledb_domain_dump(d.context.tiledbContext, d.tiledbDomain, C.stdout)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error dumping domain to stdout: %s", d.context.LastError())
	}
	return nil
}

// Dump Dumps the domain in ASCII format in the selected output.
func (d *Domain) Dump(path string) error {

	if _, err := os.Stat(path); err == nil {
		return fmt.Errorf("Error path already %s exists", path)
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
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error dumping domain to file %s: %s", path, d.context.LastError())
	}
	return nil
}
