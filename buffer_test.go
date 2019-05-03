package tiledb

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func ExampleNewBuffer() {
	// Create context with default config
	context, err := NewContext(nil)
	if err != nil {
		// Handle error
		return
	}

	// Create Buffer
	buffer, err := NewBuffer(context)
	if err != nil {
		// Handle error
		return
	}

	// Get data slice
	bytes, err := buffer.Data()
	if err != nil {
		// Handle error
		return
	}
	fmt.Println(bytes)
	// Output: []
}

// TestNewBuffer tests creating a new buffer
func TestNewBuffer(t *testing.T) {
	context, err := NewContext(nil)
	assert.Nil(t, err)

	buffer, err := NewBuffer(context)
	assert.Nil(t, err)
	assert.NotNil(t, buffer)

	bytes, err := buffer.Data()
	assert.Nil(t, err)
	assert.Nil(t, bytes)

	datatype, err := buffer.Type()
	assert.Nil(t, err)
	assert.Equal(t, datatype, TILEDB_UINT8)
}
