package tiledb

import (
	"fmt"
	"io"
	"runtime"
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
	bytes, err := buffer.dataCopy()
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

	bytes, err := buffer.dataCopy()
	require.NoError(t, err)
	assert.Nil(t, bytes)

	datatype, err := buffer.Type()
	require.NoError(t, err)
	assert.Equal(t, datatype, TILEDB_UINT8)
}

func TestBufferSafety(t *testing.T) {
	context, err := NewContext(nil)
	require.NoError(t, err)
	buffer, err := NewBuffer(context)
	require.NoError(t, err)

	require.NoError(t, buffer.SetBuffer([]byte{8, 6, 7, 5, 3, 0, 9}))

	churn := func() {
		churners := make([][]byte, 1024*128)
		for i := range churners {
			churners[i] = make([]byte, 7)
			for j := range churners[i] {
				churners[i][j] = ^byte(j)
			}
		}
		for i := range churners {
			churners[i] = nil
		}
	}
	verify := func() {
		got, err := buffer.Serialize(TILEDB_CAPNP)
		require.NoError(t, err)
		assert.Equal(t, []byte{8, 6, 7, 5, 3, 0, 9}, got)
	}

	t.Log("pre churn")
	churn()
	t.Log("post churn")
	verify()
	t.Log("pre gc")
	runtime.GC()
	t.Log("post gc")
	verify()
	t.Log("pre churn 2")
	churn()
	t.Log("post churn 2")
	verify()
	t.Log("pre gc 2")
	runtime.GC()
	t.Log("post gc 2")
	verify()
}

type byteCounter struct {
	BytesWritten int64
}

func (b *byteCounter) Write(x []byte) (int, error) {
	b.BytesWritten += int64(len(x))
	return len(x), nil
}

func TestWriteTo(t *testing.T) {
	context, err := NewContext(nil)
	require.NoError(t, err)
	buffer, err := NewBuffer(context)
	require.NoError(t, err)

	testSizes := [5]int{0, 16, 256, 65536, 268435456}
	for _, size := range testSizes {
		err := buffer.SetBuffer(make([]byte, size))
		require.NoError(t, err)

		counter := new(byteCounter)
		n, err := buffer.WriteTo(counter)
		require.NoError(t, err)
		assert.Equal(t, size, int(n))
	}
}

func TestReadAt(t *testing.T) {
	context, err := NewContext(nil)
	require.NoError(t, err)
	buffer, err := NewBuffer(context)
	require.NoError(t, err)

	err = buffer.SetBuffer([]byte{})
	require.NoError(t, err)

	n, err := buffer.ReadAt(make([]byte, 10), 0)
	require.Equal(t, io.EOF, err)
	require.Equal(t, 0, n)

	testSizes := [4]int{16, 256, 65536, 256 << 20}
	for _, size := range testSizes {
		err = buffer.SetBuffer(make([]byte, size))
		require.NoError(t, err)

		readBuffer := make([]byte, 10)
		n, err = buffer.ReadAt(readBuffer, 0)
		require.NoError(t, err)
		require.Equal(t, 10, n)

		n, err = buffer.ReadAt(readBuffer, int64(size)-10)
		require.Equal(t, io.EOF, err)
		require.Equal(t, 10, n)

		n, err = buffer.ReadAt(readBuffer, int64(size)-5)
		require.Equal(t, io.EOF, err)
		require.Equal(t, 5, n)

		n, err = buffer.ReadAt(readBuffer, int64(size))
		require.Equal(t, io.EOF, err)
		require.Equal(t, 0, n)

		n, err = buffer.ReadAt(readBuffer, int64(size)+1)
		require.Equal(t, io.EOF, err)
		require.Equal(t, 0, n)

		n, err = buffer.ReadAt(readBuffer, int64(size)+100)
		require.Equal(t, io.EOF, err)
		require.Equal(t, 0, n)

		_, err = buffer.ReadAt(readBuffer, -1)
		require.EqualError(t, err, "offset cannot be negative")
	}
}
