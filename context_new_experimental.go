//go:build experimental

package tiledb

/*
#cgo LDFLAGS: -ltiledb
#cgo linux LDFLAGS: -ldl
#include <tiledb/tiledb.h>
#include <tiledb/tiledb_experimental.h>
#include <stdlib.h>
*/
import "C"
import "fmt"

func makeContext(config *Config) (*Context, error) {
	context := &Context{}
	var ret C.int32_t
	var tdbErr *C.tiledb_error_t
	if config != nil {
		ret = C.tiledb_ctx_alloc_with_error(config.tiledbConfig, &context.tiledbContext, &tdbErr)
	} else {
		ret = C.tiledb_ctx_alloc_with_error(nil, &context.tiledbContext, &tdbErr)
	}
	if ret != C.TILEDB_OK {
		// If the error isn't null report this
		if tdbErr != nil {
			var msg *C.char
			C.tiledb_error_message(tdbErr, &msg)
			defer C.tiledb_error_free(&tdbErr)
			return nil, fmt.Errorf("error creating tiledb context: %s", C.GoString(msg))
		}
		// If the context is not null see if the error exists there
		if context.tiledbContext != nil {
			return nil, fmt.Errorf("error creating tiledb context: %w", context.LastError())
		}
		return nil, fmt.Errorf("error creating tiledb context: unknown error")
	}
	return context, nil
}
