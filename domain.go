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

type domainHandle struct{ *capiHandle }

func freeCapiDomain(c unsafe.Pointer) {
	C.tiledb_domain_free((**C.tiledb_domain_t)(unsafe.Pointer(&c)))
}

func newDomainHandle(ptr *C.tiledb_domain_t) domainHandle {
	return domainHandle{newCapiHandle(unsafe.Pointer(ptr), freeCapiDomain)}
}

func (x domainHandle) Get() *C.tiledb_domain_t {
	return (*C.tiledb_domain_t)(x.capiHandle.Get())
}

// Domain represents the domain of an array.
// A Domain defines the set of Dimension objects for a given array.
// The properties of a Domain derive from the underlying dimensions.
// A Domain is a component of an ArraySchema.
type Domain struct {
	tiledbDomain domainHandle
	context      *Context
}

func newDomainFromHandle(context *Context, handle domainHandle) *Domain {
	return &Domain{tiledbDomain: handle, context: context}
}

// NewDomain allocates a new domain.
func NewDomain(tdbCtx *Context) (*Domain, error) {
	var domainPtr *C.tiledb_domain_t
	ret := C.tiledb_domain_alloc(tdbCtx.tiledbContext.Get(), &domainPtr)
	runtime.KeepAlive(tdbCtx)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("error creating tiledb domain: %w", tdbCtx.LastError())
	}

	return newDomainFromHandle(tdbCtx, newDomainHandle(domainPtr)), nil
}

// Free releases the internal TileDB core data that was allocated on the C heap.
// It is automatically called when this object is garbage collected, but can be
// called earlier to manually release memory if needed. Free is idempotent and
// can safely be called many times on the same object; if it has already
// been freed, it will not be freed again.
func (d *Domain) Free() {
	d.tiledbDomain.Free()
}

// Context exposes the internal TileDB context used to initialize the domain.
func (d *Domain) Context() *Context {
	return d.context
}

// Type returns a domain's type deduced from dimensions.
func (d *Domain) Type() (Datatype, error) {
	var datatype C.tiledb_datatype_t
	ret := C.tiledb_domain_get_type(d.context.tiledbContext.Get(), d.tiledbDomain.Get(), &datatype)
	runtime.KeepAlive(d)
	if ret != C.TILEDB_OK {
		return -1, fmt.Errorf("error getting tiledb domain type: %w", d.context.LastError())
	}
	return Datatype(datatype), nil
}

// NDim returns the number of dimensions.
func (d *Domain) NDim() (uint, error) {
	var ndim C.uint32_t
	ret := C.tiledb_domain_get_ndim(d.context.tiledbContext.Get(), d.tiledbDomain.Get(), &ndim)
	runtime.KeepAlive(d)
	if ret != C.TILEDB_OK {
		return 0, fmt.Errorf("error getting tiledb domain number of dimensions: %w", d.context.LastError())
	}
	return uint(ndim), nil
}

// DimensionFromIndex retrieves a dimension object from a domain by index.
func (d *Domain) DimensionFromIndex(index uint) (*Dimension, error) {
	var dim *C.tiledb_dimension_t
	ret := C.tiledb_domain_get_dimension_from_index(d.context.tiledbContext.Get(),
		d.tiledbDomain.Get(), C.uint32_t(index), &dim)
	runtime.KeepAlive(d)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("error getting tiledb dimension by index for domain: %w", d.context.LastError())
	}

	return newDimensionFromHandle(d.context, newDimensionHandle(dim)), nil
}

// DimensionFromName retrieves a dimension object from a domain by name (key).
func (d *Domain) DimensionFromName(name string) (*Dimension, error) {
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))
	var dim *C.tiledb_dimension_t
	ret := C.tiledb_domain_get_dimension_from_name(d.context.tiledbContext.Get(), d.tiledbDomain.Get(), cname, &dim)
	runtime.KeepAlive(d)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("error getting tiledb dimension by name for domain: %w", d.context.LastError())
	}

	return newDimensionFromHandle(d.context, newDimensionHandle(dim)), nil
}

// AddDimensions adds one or more dimensions to a domain.
func (d *Domain) AddDimensions(dimensions ...*Dimension) error {
	for _, dimension := range dimensions {
		ret := C.tiledb_domain_add_dimension(d.context.tiledbContext.Get(), d.tiledbDomain.Get(), dimension.tiledbDimension.Get())
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
	ret := C.tiledb_domain_has_dimension(d.context.tiledbContext.Get(), d.tiledbDomain.Get(), cDimName, &hasDim)
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
	var cStr *C.tiledb_string_t
	ret := C.tiledb_domain_dump_str(d.context.tiledbContext.Get(), d.tiledbDomain.Get(), &cStr)
	runtime.KeepAlive(d)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("error dumping domain to string: %w", d.context.LastError())
	}
	defer C.tiledb_string_free(&cStr)

	var cStrPtr *C.char
	var cStrLen C.size_t
	ret = C.tiledb_string_view(cStr, &cStrPtr, &cStrLen)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("error getting string view for domain dump: %w", d.context.LastError())
	}
	goStr := C.GoStringN(cStrPtr, C.int(cStrLen))
	fmt.Print(goStr)
	return nil
}

// Dump dumps the domain in ASCII format to the given path.
func (d *Domain) Dump(path string) error {
	if _, err := os.Stat(path); err == nil {
		return fmt.Errorf("error path already %s exists", path)
	}

	var cStr *C.tiledb_string_t
	ret := C.tiledb_domain_dump_str(d.context.tiledbContext.Get(), d.tiledbDomain.Get(), &cStr)
	runtime.KeepAlive(d)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("error dumping domain to string: %w", d.context.LastError())
	}
	defer C.tiledb_string_free(&cStr)

	var cStrPtr *C.char
	var cStrLen C.size_t
	ret = C.tiledb_string_view(cStr, &cStrPtr, &cStrLen)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("error getting string view for domain dump: %w", d.context.LastError())
	}
	goStr := C.GoStringN(cStrPtr, C.int(cStrLen))

	err := os.WriteFile(path, []byte(goStr), 0644)
	if err != nil {
		return fmt.Errorf("error writing domain dump to file %s: %w", path, err)
	}
	return nil
}
