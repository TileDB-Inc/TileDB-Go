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

// Config is tiledb config
type Config struct {
	tiledbConfig *C.tiledb_config_t
}

// NewConfig alloc a new configuration
func NewConfig() (*Config, error) {
	var config Config
	var err *C.tiledb_error_t
	C.tiledb_config_alloc(&config.tiledbConfig, &err)
	if err != nil {
		var msg *C.char
		defer C.free(unsafe.Pointer(msg))
		C.tiledb_error_message(err, &msg)
		defer C.tiledb_error_free(&err)
		return nil, fmt.Errorf("Error creating tiledb config: %s", C.GoString(msg))
	}

	// Set finalizer for free C pointer on gc
	runtime.SetFinalizer(&config, func(config *Config) {
		config.Free()
	})

	return &config, nil
}

// Set configuration parameter
func (c *Config) Set(param string, value string) error {
	var err *C.tiledb_error_t
	C.tiledb_config_set(c.tiledbConfig, C.CString(param), C.CString(value), &err)

	if err != nil {
		var msg *C.char
		defer C.free(unsafe.Pointer(msg))
		C.tiledb_error_message(err, &msg)
		defer C.tiledb_error_free(&err)
		return fmt.Errorf("Error setting %s:%s in config: %s", param, value, C.GoString(msg))
	}

	return nil
}

// Get configuration parameter
func (c *Config) Get(param string) (string, error) {
	var err *C.tiledb_error_t
	var val *C.char
	defer C.free(unsafe.Pointer(val))
	C.tiledb_config_get(c.tiledbConfig, C.CString(param), &val, &err)

	if err != nil {
		var msg *C.char
		defer C.free(unsafe.Pointer(msg))
		C.tiledb_error_message(err, &msg)
		defer C.tiledb_error_free(&err)
		return "", fmt.Errorf("Error getting %s in config: %s", param, C.GoString(msg))
	}

	value := C.GoString(val)

	return value, nil
}

// Unset sets a parameter back to default value
func (c *Config) Unset(param string) error {
	var err *C.tiledb_error_t
	C.tiledb_config_unset(c.tiledbConfig, C.CString(param), &err)

	if err != nil {
		var msg *C.char
		defer C.free(unsafe.Pointer(msg))
		C.tiledb_error_message(err, &msg)
		defer C.tiledb_error_free(&err)
		return fmt.Errorf("Error unsetting %s in config: %s", param, C.GoString(msg))
	}

	return nil
}

// LoadFromFile reads a configuration text file
func (c *Config) LoadFromFile(file string) error {
	var err *C.tiledb_error_t
	C.tiledb_config_load_from_file(c.tiledbConfig, C.CString(file), &err)

	if err != nil {
		var msg *C.char
		defer C.free(unsafe.Pointer(msg))
		C.tiledb_error_message(err, &msg)
		defer C.tiledb_error_free(&err)
		return fmt.Errorf("Error loading config from file %s: %s", file, C.GoString(msg))
	}

	return nil
}

// SaveToFile reads a configuration text file
func (c *Config) SaveToFile(file string) error {
	var err *C.tiledb_error_t
	C.tiledb_config_save_to_file(c.tiledbConfig, C.CString(file), &err)

	if err != nil {
		var msg *C.char
		defer C.free(unsafe.Pointer(msg))
		C.tiledb_error_message(err, &msg)
		defer C.tiledb_error_free(&err)
		return fmt.Errorf("Error saving config from file %s: %s", file, C.GoString(msg))
	}

	return nil
}

// Free tiledb_config_t that was allocated on heap in c
func (c *Config) Free() {
	if c.tiledbConfig != nil {
		C.tiledb_config_free(&c.tiledbConfig)
	}
}
