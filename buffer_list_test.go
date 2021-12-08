package tiledb

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func ExampleNewBufferList() {
	// Create context with default config
	context, err := NewContext(nil)
	if err != nil {
		// Handle error
		return
	}

	// Create BufferList
	bufferList, err := NewBufferList(context)
	if err != nil {
		// Handle error
		return
	}

	// Get num buffers
	numBuffers, err := bufferList.NumBuffers()
	if err != nil {
		// Handle error
		return
	}
	fmt.Println(numBuffers)
	// Output: 0
}

// TestNewBufferList tests creating a new bufferList
func TestNewBufferList(t *testing.T) {
	context, err := NewContext(nil)
	require.NoError(t, err)

	bufferList, err := NewBufferList(context)
	require.NoError(t, err)
	assert.NotNil(t, bufferList)

	numBuffers, err := bufferList.NumBuffers()
	require.NoError(t, err)
	assert.Equal(t, uint64(0), numBuffers)

	totalSize, err := bufferList.TotalSize()
	require.NoError(t, err)
	assert.Equal(t, uint64(0), totalSize)
}
