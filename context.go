package tiledb

/*
#include <tiledb/tiledb.h>
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

type contextHandle struct{ *capiHandle }

func freeCapiContext(c unsafe.Pointer) {
	C.tiledb_ctx_free((**C.tiledb_ctx_t)(unsafe.Pointer(&c)))
}

func newContextHandle(ptr *C.tiledb_ctx_t) contextHandle {
	return contextHandle{newCapiHandle(unsafe.Pointer(ptr), freeCapiContext)}
}

func (x contextHandle) Get() *C.tiledb_ctx_t {
	return (*C.tiledb_ctx_t)(x.capiHandle.Get())
}

// Context A TileDB context wraps a TileDB storage manager “instance.” Most
// objects and functions will require a Context.
// Internal error handling is also defined by the Context;
// the default error handler throws a TileDBError with a specific message.
type Context struct {
	tiledbContext contextHandle
}

func newContextFromHandle(handle contextHandle) *Context {
	return &Context{tiledbContext: handle}
}

// NewContext creates a TileDB context with the given configuration.
// If the configuration passed is nil, it is created with the default config.
func NewContext(config *Config) (*Context, error) {
	var contextPtr *C.tiledb_ctx_t
	var ret C.int32_t
	var tdbErr *C.tiledb_error_t
	if config != nil {
		ret = C.tiledb_ctx_alloc_with_error(config.tiledbConfig.Get(), &contextPtr, &tdbErr)
	} else {
		ret = C.tiledb_ctx_alloc_with_error(nil, &contextPtr, &tdbErr)
	}
	runtime.KeepAlive(config)
	if ret != C.TILEDB_OK {
		// If the error isn't null report this
		if tdbErr != nil {
			var msg *C.char
			C.tiledb_error_message(tdbErr, &msg)
			defer C.tiledb_error_free(&tdbErr)
			return nil, fmt.Errorf("error creating tiledb context: %s", C.GoString(msg))
		}
		return nil, fmt.Errorf("error creating tiledb context: unknown error")
	}
	context := newContextFromHandle(newContextHandle(contextPtr))

	err := context.setDefaultTags()
	if err != nil {
		return nil, fmt.Errorf("error creating tiledb context: %w", err)
	}

	return context, nil
}

// NewContextFromMap creates a TileDB context with the given configuration.
// If the configuration passed is nil, it is created with the default config.
// This is a shortcut for creating a *Config from the given map and
// using it to create a new context.
func NewContextFromMap(cfgMap map[string]string) (*Context, error) {
	if cfgMap == nil {
		return NewContext(nil)
	}
	config, err := NewConfig()
	if err != nil {
		return nil, err
	}
	defer config.Free()
	for k, v := range cfgMap {
		if err := config.Set(k, v); err != nil {
			// The value is not included in the error message in case it is sensitive,
			// like a password or access key.
			return nil, fmt.Errorf("error setting config value %q: %w", k, err)
		}
	}
	return NewContext(config)
}

// Free releases the internal TileDB core data that was allocated on the C heap.
// It is automatically called when this object is garbage collected, but can be
// called earlier to manually release memory if needed. Free is idempotent and
// can safely be called many times on the same object; if it has already
// been freed, it will not be freed again.
func (c *Context) Free() {
	c.tiledbContext.Free()
}

// CancelAllTasks cancels all currently executing tasks on the context.
func (c *Context) CancelAllTasks() error {
	ret := C.tiledb_ctx_cancel_tasks(c.tiledbContext.Get())
	if ret != C.TILEDB_OK {
		return errors.New("failed to cancel tasks")
	}
	return nil
}

// Config retrieves a copy of the config from context.
func (c *Context) Config() (*Config, error) {
	var configPtr *C.tiledb_config_t
	ret := C.tiledb_ctx_get_config(c.tiledbContext.Get(), &configPtr)
	runtime.KeepAlive(c)

	if ret == C.TILEDB_OOM {
		return nil, errors.New("out of Memory error in GetConfig")
	} else if ret != C.TILEDB_OK {
		return nil, errors.New("unknown error in GetConfig")
	}

	return newConfigFromHandle(newConfigHandle(configPtr)), nil
}

// LastError returns the last error from this context.
func (c *Context) LastError() error {
	var err *C.tiledb_error_t
	ret := C.tiledb_ctx_get_last_error(c.tiledbContext.Get(), &err)
	runtime.KeepAlive(c)
	if ret == C.TILEDB_OOM {
		return errors.New("out of Memory error in tiledb_ctx_get_last_error")
	} else if ret != C.TILEDB_OK {
		return errors.New("unknown error in tiledb_ctx_get_last_error")
	}

	if err != nil {
		defer C.tiledb_error_free(&err)
		return cError(err)
	}
	return nil
}

// IsSupportedFS returns true if the given filesystem backend is supported.
func (c *Context) IsSupportedFS(fs FS) (bool, error) {
	var isSupported C.int32_t
	ret := C.tiledb_ctx_is_supported_fs(c.tiledbContext.Get(), C.tiledb_filesystem_t(fs), &isSupported)
	runtime.KeepAlive(c)

	if ret != C.TILEDB_OK {
		return false, errors.New("error in checking FS support")
	}

	return isSupported != 0, nil
}

// SetTag sets the context tag.
func (c *Context) SetTag(key string, value string) error {
	ckey := C.CString(key)
	defer C.free(unsafe.Pointer(ckey))
	cvalue := C.CString(value)
	defer C.free(unsafe.Pointer(cvalue))

	ret := C.tiledb_ctx_set_tag(c.tiledbContext.Get(), ckey, cvalue)
	runtime.KeepAlive(c)

	if ret != C.TILEDB_OK {
		return fmt.Errorf("error in setting tag: %w", c.LastError())
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

// Stats gets stats for a context as json bytes.
func (c *Context) Stats() ([]byte, error) {
	var stats *C.char
	if ret := C.tiledb_ctx_get_stats(c.tiledbContext.Get(), &stats); ret != C.TILEDB_OK {
		return nil, fmt.Errorf("error getting stats from context: %w", c.LastError())
	}
	runtime.KeepAlive(c)

	s := C.GoString(stats)
	if ret := C.tiledb_stats_free_str(&stats); ret != C.TILEDB_OK {
		return nil, errors.New("error freeing string from dumping stats to string")
	}

	if s == "" {
		return []byte("{}"), nil
	}

	return []byte(s), nil
}
