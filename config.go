package tiledb

/*
#include <tiledb/tiledb.h>
#include <stdlib.h>
*/
import "C"

import (
	"errors"
	"fmt"
	"runtime"
	"unsafe"
)

// Config carries configuration parameters for a context.
type Config struct {
	tiledbConfig *C.tiledb_config_t
}

// NewConfig allocates a new configuration.
func NewConfig() (*Config, error) {
	var config Config
	var err *C.tiledb_error_t
	C.tiledb_config_alloc(&config.tiledbConfig, &err)
	if err != nil {
		defer C.tiledb_error_free(&err)
		return nil, fmt.Errorf("error creating tiledb config: %w", cError(err))
	}
	runtime.AddCleanup(&config, freeFreeable, Freeable(&config))

	return &config, nil
}

// Set sets a config parameter-value pair.
func (c *Config) Set(param string, value string) error {
	var err *C.tiledb_error_t
	cparam := C.CString(param)
	defer C.free(unsafe.Pointer(cparam))
	cvalue := C.CString(value)
	defer C.free(unsafe.Pointer(cvalue))
	C.tiledb_config_set(c.tiledbConfig, cparam, cvalue, &err)
	runtime.KeepAlive(c)

	if err != nil {
		defer C.tiledb_error_free(&err)
		return fmt.Errorf("error setting %s:%s in config: %w", param, value, cError(err))
	}

	return nil
}

// Get gets a parameter from the configuration by key.
func (c *Config) Get(param string) (string, error) {
	var err *C.tiledb_error_t
	var cvalue *C.char // c must be kept alive while cvalue is being accessed.
	cparam := C.CString(param)
	defer C.free(unsafe.Pointer(cparam))
	C.tiledb_config_get(c.tiledbConfig, cparam, &cvalue, &err)

	if err != nil {
		defer C.tiledb_error_free(&err)
		return "", fmt.Errorf("error getting %s in config: %w", param, cError(err))
	}

	value := C.GoString(cvalue)
	runtime.KeepAlive(c)

	return value, nil
}

// Unset resets a config parameter to its default value.
func (c *Config) Unset(param string) error {
	var err *C.tiledb_error_t
	cparam := C.CString(param)
	defer C.free(unsafe.Pointer(cparam))
	C.tiledb_config_unset(c.tiledbConfig, cparam, &err)
	runtime.KeepAlive(c)

	if err != nil {
		defer C.tiledb_error_free(&err)
		return fmt.Errorf("error unsetting %s in config: %w", param, cError(err))
	}

	return nil
}

// SaveToFile saves the config parameters to a (local) text file.
func (c *Config) SaveToFile(file string) error {
	var err *C.tiledb_error_t
	cfile := C.CString(file)
	defer C.free(unsafe.Pointer(cfile))
	C.tiledb_config_save_to_file(c.tiledbConfig, cfile, &err)
	runtime.KeepAlive(c)

	if err != nil {
		defer C.tiledb_error_free(&err)
		return fmt.Errorf("error saving config from file %s: %w", file, cError(err))
	}

	return nil
}

// LoadConfig reads a configuration from the given uri.
func LoadConfig(uri string) (*Config, error) {

	if uri == "" {
		return nil, errors.New("error loading tiledb config: passed uri is empty")
	}

	var config Config
	var err *C.tiledb_error_t
	C.tiledb_config_alloc(&config.tiledbConfig, &err)
	if err != nil {
		defer C.tiledb_error_free(&err)
		return nil, fmt.Errorf("error loading tiledb config: %w", cError(err))
	}

	curi := C.CString(uri)
	defer C.free(unsafe.Pointer(curi))
	C.tiledb_config_load_from_file(config.tiledbConfig, curi, &err)
	if err != nil {
		defer C.tiledb_error_free(&err)
		return nil, fmt.Errorf("error loading config from file %s: %w", uri, cError(err))
	}
	runtime.AddCleanup(&config, freeFreeable, Freeable(&config))

	return &config, nil
}

// Free releases the internal TileDB core data that was allocated on the C heap.
// It is automatically called when this object is garbage collected, but can be
// called earlier to manually release memory if needed. Free is idempotent and
// can safely be called many times on the same object; if it has already
// been freed, it will not be freed again.
func (c *Config) Free() {
	if c.tiledbConfig != nil {
		C.tiledb_config_free(&c.tiledbConfig)
	}
}

// Iterate iterates over configuration.
//
//	for iter, err := config.Iterate(); !iter.Done(); iter.Next(){
//	   param, value, err := iter.Here()
//	}
func (c *Config) Iterate(prefix string) (*ConfigIter, error) {
	return NewConfigIter(c, prefix)
}

// Cmp compares two configs.
func (c *Config) Cmp(other *Config) bool {
	var equal C.uint8_t
	C.tiledb_config_compare(c.tiledbConfig, other.tiledbConfig, &equal)
	runtime.KeepAlive(c)
	runtime.KeepAlive(other)

	return equal == 1
}
