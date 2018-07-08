package tiledb

/*
#cgo LDFLAGS: -ltiledb
#include <tiledb/tiledb.h>
#include <stdlib.h>
*/
import "C"

import (
	"fmt"
	"runtime"
	"unsafe"
)

// Context A TileDB context wraps a TileDB storage manager “instance.” Most
// objects and functions will require a Context.
// Internal error handling is also defined by the Context;
// the default error handler throws a TileDBError with a specific message.
type Context struct {
	tiledbContext *C.tiledb_ctx_t
}

// NewContext creates a TileDB context with the given configuration
// If the configuration passed is null it is created with default config
func NewContext(config *Config) (*Context, error) {
	var context Context
	var err *C.tiledb_error_t
	if config != nil {
		C.tiledb_ctx_alloc(config.tiledbConfig, &context.tiledbContext)
	} else {
		C.tiledb_ctx_alloc(nil, &context.tiledbContext)
	}
	if err != nil {
		var msg *C.char
		defer C.free(unsafe.Pointer(msg))
		C.tiledb_error_message(err, &msg)
		defer C.tiledb_error_free(&err)
		return nil, fmt.Errorf("Error creating tiledb context: %s", C.GoString(msg))
	}

	// Set finalizer for free C pointer on gc
	runtime.SetFinalizer(&context, func(context *Context) {
		context.Free()
	})

	return &context, nil
}

// Free tiledb_ctx_t that was allocated on heap in c
func (c *Context) Free() {
	if c.tiledbContext != nil {
		C.tiledb_ctx_free(&c.tiledbContext)
	}
}

// GetConfig retrieves a copy of the config from context
func (c *Context) GetConfig() (*Config, error) {
	config := &Config{}
	ret := C.tiledb_ctx_get_config(c.tiledbContext, &config.tiledbConfig)

	if ret == C.TILEDB_OOM {
		return nil, fmt.Errorf("Out of Memory error in GetConfig")
	} else if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Unknown error in GetConfig")
	}

	return config, nil
}

// GetLastError returns the last error from this context
func (c *Context) GetLastError() error {
	var err *C.tiledb_error_t
	C.tiledb_ctx_get_last_error(c.tiledbContext, &err)

	if err != nil {
		var msg *C.char
		defer C.free(unsafe.Pointer(msg))
		defer C.tiledb_error_free(&err)
		C.tiledb_error_message(err, &msg)
		return fmt.Errorf("%s", C.GoString(msg))
	}
	return nil
}

// IsFSSupported Return true if the given filesystem backend is supported.
func (c *Context) IsFSSupported(fs FS) (bool, error) {
	var isSupported C.int
	ret := C.tiledb_ctx_is_supported_fs(c.tiledbContext, C.tiledb_filesystem_t(fs), &isSupported)

	if ret != C.TILEDB_OK {
		return false, fmt.Errorf("Error in checking FS support")
	}

	if isSupported == 0 {
		return false, nil
	}

	return true, nil
}
