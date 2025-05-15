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
)

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
