package tiledb

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ExampleNewContext example of creating a new context
func ExampleNewContext() {

	// Create Context with default configuration:
	context, err := NewContext(nil)
	if err != nil {
		// handle error
		return
	}

	// Create a config and use it to create a new Context:
	// (See ExampleConfig_Set for an example of setting config variables.)
	config, err := NewConfig()
	if err != nil {
		// handle error
		return
	}
	context, err = NewContext(config)
	if err != nil {
		// handle error
		return
	}

	// Create a context directly from a configuration map:
	context, err = NewContextFromMap(map[string]string{
		"sm.memory_budget":     "17179869184", // 16 GiB
		"sm.memory_budget_var": "34359738368", // 32 GiB
	})
	if err != nil {
		// handle error
		return
	}

	stats, err := context.Stats()
	if err != nil {
		// Handle error
		return
	}

	if len(stats) > 0 {
		// Do something with stats
	}

	// Check if S3 is supported:
	isS3Supported, err := context.IsSupportedFS(TILEDB_S3)
	if err != nil {
		// handle error
		return
	}
	// Output: true
	fmt.Println(isS3Supported)
}

// TestNewContext tests setting a new context
func TestNewContext(t *testing.T) {
	context, err := NewContext(nil)

	require.NoError(t, err)
	// Test freeing c allocs
	context.Free()

	config, err := NewConfig()
	require.NoError(t, err)

	// Test context with config
	context, err = NewContext(config)
	require.NoError(t, err)
	assert.NotNil(t, context)
}

// TestNewContext tests setting a new context
func TestCancelAllTasks(t *testing.T) {
	config, err := NewConfig()
	assert.Nil(t, err)

	context, err := NewContext(config)
	assert.Nil(t, err)
	assert.NotNil(t, context)

	// just call cancel with no tasks
	assert.NoError(t, context.CancelAllTasks())
}

// TestGetContextConfig tests creating a new Context with config vars.
func TestGetContextConfig(t *testing.T) {
	// Create a context with a non-default value:
	context, err := NewContextFromMap(map[string]string{
		"sm.memory_budget": "4294967296",
	})
	require.NoError(t, err)
	config, err := context.Config()
	require.NoError(t, err)

	// Validate config has setting changed
	val, err := config.Get("sm.memory_budget")
	require.NoError(t, err)
	assert.Equal(t, "4294967296", val)
}

// TestContextLastError tests retrieving the last error
func TestContextLastError(t *testing.T) {
	context, err := NewContext(nil)
	require.NoError(t, err)
	ctxErr := context.LastError()
	assert.Nil(t, ctxErr)
}

// TestContextIsFSSupported tests if we can detect filesystem support properly
func TestContextIsFSSupported(t *testing.T) {
	context, err := NewContext(nil)
	require.NoError(t, err)
	_, ctxErr := context.IsSupportedFS(TILEDB_S3)
	assert.Nil(t, ctxErr)
}

func TestContextSetTag(t *testing.T) {
	context, err := NewContext(nil)
	require.NoError(t, err)
	require.NoError(t, context.SetTag("key", "value"))
}
