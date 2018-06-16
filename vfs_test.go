package tiledb

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

// TestVFS validates vfs file operations are successful
func TestVFS(t *testing.T) {
	config, err := NewConfig()
	assert.Nil(t, err)

	context, err := NewContext(config)
	assert.Nil(t, err)

	vfs, err := NewVFS(context, config)
	assert.Nil(t, err)

	tmpPath := os.TempDir() + string(os.PathSeparator) + "tiledb_test_vfs"
	defer os.Remove(tmpPath)
	if _, err = os.Stat(tmpPath); err == nil {
		os.Remove(tmpPath)
	}

	isFile, err := vfs.IsFile(tmpPath)
	assert.Nil(t, err)
	assert.False(t, isFile)

	isDir, err := vfs.IsDir(tmpPath)
	assert.Nil(t, err)
	assert.False(t, isDir)

	// Create directory
	err = vfs.CreateDir(tmpPath)
	assert.Nil(t, err)

	isDir, err = vfs.IsDir(tmpPath)
	assert.Nil(t, err)
	assert.True(t, isDir)

	// Remove directory
	err = vfs.RemoveDir(tmpPath)
	assert.Nil(t, err)

	// Create File
	err = vfs.Touch(tmpPath)
	assert.Nil(t, err)

	fh, err := vfs.Open(tmpPath, TILEDB_VFS_WRITE)
	assert.Nil(t, err)

	bytes := []byte{0, 1, 2}
	err = vfs.Write(fh, bytes)
	assert.Nil(t, err)

	bytes2, err := vfs.Read(fh, 0, uint64(len(bytes)))

	assert.EqualValues(t, bytes, bytes2)

	// Remove File
	err = vfs.RemoveFile(tmpPath)
	assert.Nil(t, err)
}

// ExampleNewVFS show basic usage of tiledb's vfs functionality
func ExampleNewVFS() {
	// Create a new config
	config, err := NewConfig()
	if err != nil {
		// return err
	}
	// Optionally set config settings here
	// config.Set("key", "value")

	// Create a context
	context, err := NewContext(config)
	if err != nil {
		// return err
	}

	// Create a VFS instance
	vfs, err := NewVFS(context, config)
	if err != nil {
		// return err
	}

	uri := "file:///tmp/tiledb_example_folder"
	// Check if directory exists
	if isDir, err := vfs.IsDir(uri); err != nil {
		fmt.Println(err)
	} else {
		// Directory exists
		if isDir {
			fmt.Println("URI is a directory")
			// Output: URI is a directory
		} else {
			fmt.Println("URI is not a directory")
			// Output: URI is not a directory
		}
	}
}
