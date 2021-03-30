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

	err1 := context.setDefaultTags()
	if err != nil {
		return nil, fmt.Errorf("Error creating tiledb context: %s", err1.Error())
	}

	return &context, nil
}

// Free tiledb_ctx_t that was allocated on heap in c
func (c *Context) Free() {
	if c.tiledbContext != nil {
		C.tiledb_ctx_free(&c.tiledbContext)
	}
}

// Config retrieves a copy of the config from context
func (c *Context) Config() (*Config, error) {
	config := Config{}
	ret := C.tiledb_ctx_get_config(c.tiledbContext, &config.tiledbConfig)

	if ret == C.TILEDB_OOM {
		return nil, fmt.Errorf("Out of Memory error in GetConfig")
	} else if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Unknown error in GetConfig")
	}

	// Set finalizer for free C pointer on gc
	runtime.SetFinalizer(&config, func(config *Config) {
		config.Free()
	})

	return &config, nil
}

// LastError returns the last error from this context
func (c *Context) LastError() error {
	var err *C.tiledb_error_t
	ret := C.tiledb_ctx_get_last_error(c.tiledbContext, &err)

	if ret == C.TILEDB_OOM {
		return fmt.Errorf("Out of Memory error in tiledb_ctx_get_last_error")
	} else if ret != C.TILEDB_OK {
		return fmt.Errorf("Unknown error in tiledb_ctx_get_last_error")
	}

	if err != nil {
		var msg *C.char
		defer C.tiledb_error_free(&err)
		ret := C.tiledb_error_message(err, &msg)

		if ret == C.TILEDB_OOM {
			return fmt.Errorf("Out of Memory error in tiledb_error_message")
		} else if ret != C.TILEDB_OK {
			return fmt.Errorf("Unknown error in tiledb_error_message")
		}

		return fmt.Errorf("%s", C.GoString(msg))
	}
	return nil
}

// IsSupportedFS Return true if the given filesystem backend is supported.
func (c *Context) IsSupportedFS(fs FS) (bool, error) {
	var isSupported C.int32_t
	ret := C.tiledb_ctx_is_supported_fs(c.tiledbContext, C.tiledb_filesystem_t(fs), &isSupported)

	if ret != C.TILEDB_OK {
		return false, fmt.Errorf("Error in checking FS support")
	}

	if isSupported == 0 {
		return false, nil
	}

	return true, nil
}

// SetTag, sets context tag
func (c *Context) SetTag(key string, value string) error {
	ckey := C.CString(key)
	defer C.free(unsafe.Pointer(ckey))
	cvalue := C.CString(value)
	defer C.free(unsafe.Pointer(cvalue))

	ret := C.tiledb_ctx_set_tag(c.tiledbContext, ckey, cvalue)

	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error in setting tag")
	}

	return nil
}

func (c *Context) setDefaultTags() error {
	err := c.SetTag("x-tiledb-api-language", "go")
	if err != nil {
		return err
	}

	err = c.SetTag("x-tiledb-api-language-version", "0.8.0")
	if err != nil {
		return err
	}

	err = c.SetTag("x-tiledb-api-sys-platform", fmt.Sprintf("%s/%s", runtime.GOOS, runtime.GOARCH))
	if err != nil {
		return err
	}

	return nil
}
