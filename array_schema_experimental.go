package tiledb

/*
#include <tiledb/tiledb.h>
#include <tiledb/tiledb_experimental.h>
#include <stdlib.h>
*/
import "C"
import (
	"fmt"
	"runtime"
	"time"
)

// NewArraySchemaAtTime allocates a new ArraySchema at the provided createTime.
func NewArraySchemaAtTime(tdbCtx *Context, arrayType ArrayType, createTime time.Time) (*ArraySchema, error) {
	return NewArraySchemaAtTimestamp(tdbCtx, arrayType, uint64(createTime.UnixMilli()))
}

// NewArraySchemaAtTimestamp allocates a new ArraySchema at the provided timestamp.
func NewArraySchemaAtTimestamp(tdbCtx *Context, arrayType ArrayType, timestamp uint64) (*ArraySchema, error) {
	var arraySchemaPtr *C.tiledb_array_schema_t
	var cTimestamp C.uint64_t = C.uint64_t(timestamp)
	ret := C.tiledb_array_schema_alloc_at_timestamp(tdbCtx.tiledbContext.Get(), C.tiledb_array_type_t(arrayType),
		cTimestamp, &arraySchemaPtr)
	runtime.KeepAlive(tdbCtx)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("error creating tiledb arraySchema: %w", tdbCtx.LastError())
	}
	return newArraySchemaFromHandle(tdbCtx, newArraySchemaHandle(arraySchemaPtr)), nil
}

// TimestampRange gets the timestamp range for the array schema.
func (a *ArraySchema) TimestampRange() (uint64, uint64, error) {
	var lo C.uint64_t
	var hi C.uint64_t
	ret := C.tiledb_array_schema_timestamp_range(a.context.tiledbContext.Get(), a.tiledbArraySchema.Get(), &lo, &hi)
	runtime.KeepAlive(a)
	if ret != C.TILEDB_OK {
		return 0, 0, fmt.Errorf("error getting timestamp range: %w", a.context.LastError())
	}
	return uint64(lo), uint64(hi), nil
}
