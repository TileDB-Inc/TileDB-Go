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

// Config Carries configuration parameters for a context.
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

// Set Sets a config parameter-value pair.
func (c *Config) Set(param string, value string) error {
	var err *C.tiledb_error_t
	cparam := C.CString(param)
	defer C.free(unsafe.Pointer(cparam))
	cvalue := C.CString(value)
	defer C.free(unsafe.Pointer(cvalue))
	C.tiledb_config_set(c.tiledbConfig, cparam, cvalue, &err)

	if err != nil {
		var msg *C.char
		C.tiledb_error_message(err, &msg)
		defer C.tiledb_error_free(&err)
		return fmt.Errorf("Error setting %s:%s in config: %s", param, value, C.GoString(msg))
	}

	return nil
}

// Get Get a parameter from the configuration by key.
func (c *Config) Get(param string) (string, error) {
	var err *C.tiledb_error_t
	var val *C.char
	cparam := C.CString(param)
	defer C.free(unsafe.Pointer(cparam))
	C.tiledb_config_get(c.tiledbConfig, cparam, &val, &err)

	if err != nil {
		var msg *C.char
		C.tiledb_error_message(err, &msg)
		defer C.tiledb_error_free(&err)
		return "", fmt.Errorf("Error getting %s in config: %s", param, C.GoString(msg))
	}

	value := C.GoString(val)

	return value, nil
}

// Unset Resets a config parameter to its default value.
func (c *Config) Unset(param string) error {
	var err *C.tiledb_error_t
	cparam := C.CString(param)
	defer C.free(unsafe.Pointer(cparam))
	C.tiledb_config_unset(c.tiledbConfig, cparam, &err)

	if err != nil {
		var msg *C.char
		C.tiledb_error_message(err, &msg)
		defer C.tiledb_error_free(&err)
		return fmt.Errorf("Error unsetting %s in config: %s", param, C.GoString(msg))
	}

	return nil
}

// SaveToFile Saves the config parameters to a (local) text file.
func (c *Config) SaveToFile(file string) error {
	var err *C.tiledb_error_t
	cfile := C.CString(file)
	defer C.free(unsafe.Pointer(cfile))
	C.tiledb_config_save_to_file(c.tiledbConfig, cfile, &err)

	if err != nil {
		var msg *C.char
		C.tiledb_error_message(err, &msg)
		defer C.tiledb_error_free(&err)
		return fmt.Errorf("Error saving config from file %s: %s", file, C.GoString(msg))
	}

	return nil
}

// LoadConfig reads a configuration from a given uri
func LoadConfig(uri string) (*Config, error) {

	if uri == "" {
		return nil, fmt.Errorf("Error loading tiledb config: passed uri is empty")
	}

	var config Config
	var err *C.tiledb_error_t
	C.tiledb_config_alloc(&config.tiledbConfig, &err)
	if err != nil {
		var msg *C.char
		C.tiledb_error_message(err, &msg)
		defer C.tiledb_error_free(&err)
		return nil, fmt.Errorf("Error loading tiledb config: %s", C.GoString(msg))
	}

	curi := C.CString(uri)
	defer C.free(unsafe.Pointer(curi))
	C.tiledb_config_load_from_file(config.tiledbConfig, curi, &err)
	if err != nil {
		var msg *C.char
		C.tiledb_error_message(err, &msg)
		defer C.tiledb_error_free(&err)
		return nil, fmt.Errorf("Error loading config from file %s: %s", uri, C.GoString(msg))
	}

	// Set finalizer for free C pointer on gc
	runtime.SetFinalizer(&config, func(config *Config) {
		config.Free()
	})

	return &config, nil
}

// Free tiledb_config_t that was allocated on heap in c
func (c *Config) Free() {
	if c.tiledbConfig != nil {
		C.tiledb_config_free(&c.tiledbConfig)
	}
}

// Iterate configuration
// for iter, err := config.Iterate(); !iter.Done(); iter.Next(){
//    param, value, err := iter.Here()
// }
func (c *Config) Iterate(prefix string) (*ConfigIter, error) {
	return NewConfigIter(c, prefix)
}
