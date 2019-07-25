package tiledb

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
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
	assert.Nil(t, err)

	bufferList, err := NewBufferList(context)
	assert.Nil(t, err)
	assert.NotNil(t, bufferList)

	numBuffers, err := bufferList.NumBuffers()
	assert.Nil(t, err)
	assert.Equal(t, uint(0), numBuffers)

	totalSize, err := bufferList.TotalSize()
	assert.Nil(t, err)
	assert.Equal(t, uint64(0), totalSize)
}
