package tiledb

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

// ExampleNewContext example of creating a new context
func ExampleNewContext() {

	// Create Context with default configuration
	context, err := NewContext(nil)

	if err != nil {
		// handle error
		return
	}

	// Create a config
	config, err := NewConfig()
	if err != nil {
		// handle error
		return
	}

	// Use created config to create a new Context
	context, err = NewContext(config)
	if err != nil {
		// handle error
		return
	}

	// Check if S3 is supported
	isS3Supported, err := context.IsSupportedFS(TILEDB_S3)
	if err != nil {
		// handle error
		return
	}
	// Output: false
	fmt.Println(isS3Supported)
}

// TestNewContext tests setting a new context
func TestNewContext(t *testing.T) {
	context, err := NewContext(nil)

	assert.Nil(t, err)
	// Test freeing c allocs
	context.Free()

	config, err := NewConfig()
	assert.Nil(t, err)

	// Test context with config
	context, err = NewContext(config)
	assert.Nil(t, err)
}

// TestGetContextConfig tests setting a new context
func TestGetContextConfig(t *testing.T) {
	// Create config and modify a default value
	config, err := NewConfig()
	assert.Nil(t, err)
	err = config.Set("sm.tile_cache_size", "10")
	assert.Nil(t, err)

	val, err := config.Get("sm.tile_cache_size")
	assert.Nil(t, err)
	assert.Equal(t, "10", val)

	// Test context with config
	context, err := NewContext(config)
	assert.Nil(t, err)

	// Get config
	config2, err := context.Config()
	assert.Nil(t, err)

	// Validate config has setting changed
	val, err = config2.Get("sm.tile_cache_size")
	assert.Nil(t, err)
	assert.Equal(t, "10", val)
}

// TestContextLastError tests retrieving the last error
func TestContextLastError(t *testing.T) {
	context, err := NewContext(nil)
	assert.Nil(t, err)
	ctxErr := context.LastError()
	assert.Nil(t, ctxErr)
}

// TestContextIsFSSupported tests if we can detect filesystem support properly
func TestContextIsFSSupported(t *testing.T) {
	context, err := NewContext(nil)
	assert.Nil(t, err)
	_, ctxErr := context.IsSupportedFS(TILEDB_S3)
	assert.Nil(t, ctxErr)
}
