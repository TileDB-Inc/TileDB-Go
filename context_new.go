//go:build !experimental

package tiledb

/*
#cgo LDFLAGS: -ltiledb
#cgo linux LDFLAGS: -ldl
#include <tiledb/tiledb.h>
#include <stdlib.h>
*/
import "C"
import "fmt"

// makeContext wraps the internals of context making. It is separated out
// so the functions which use experimental APIs are separate from those
// that do not.
func makeContext(config *Config) (*Context, error) {
	context := &Context{}
	var ret C.int32_t

	if config != nil {
		ret = C.tiledb_ctx_alloc(config.tiledbConfig, &context.tiledbContext)
	} else {
		ret = C.tiledb_ctx_alloc(nil, &context.tiledbContext)
	}
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("error creating tiledb context: %w", context.LastError())
	}
	return context, nil
}
