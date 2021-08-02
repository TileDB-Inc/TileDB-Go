package tiledb

import (
	"fmt"
	"runtime"
	"runtime/debug"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	require.NoError(t, err)

	buffer, err := NewBuffer(context)
	require.NoError(t, err)
	assert.NotNil(t, buffer)

	bytes, err := buffer.Data()
	require.NoError(t, err)
	assert.Nil(t, bytes)

	datatype, err := buffer.Type()
	require.NoError(t, err)
	assert.Equal(t, datatype, TILEDB_UINT8)
}

// TestBufferMemory tests holding on to a buffer's memory
func TestBufferMemory(t *testing.T) {
	// Disable garbage collection for this test.
	// TODO: Pull this out into a "disableGC" helper using t.Cleanup
	// when we stop supporting Go 1.13.
	was := debug.SetGCPercent(-1)
	defer debug.SetGCPercent(was)

	tdbCtx, err := NewContext(nil)
	require.NoError(t, err)
	buf, err := SerializeArraySchema(buildArraySchema(tdbCtx, t), TILEDB_JSON, false)
	require.NoError(t, err)
	data, err := buf.Data()
	require.NoError(t, err)
	correct := string(data) // this copies the data buffer into a string.
	runtime.KeepAlive(buf)
	churn()
	runtime.GC()
	now := string(data)
	assert.Equal(t, correct, now)
}

// churn wastes lots of RAM a few times over.
func churn() {
	for i := 0; i < 4; i++ {
		waste := make([][]byte, 64)
		for i := range waste {
			waste[i] = make([]byte, 16*1024*1024)
		}
		waste = nil
		runtime.GC()
	}
}
