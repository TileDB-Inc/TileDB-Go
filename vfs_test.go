package tiledb

import (
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
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

	tmpFilePath := os.TempDir() + string(os.PathSeparator) + "tiledb_test_vfs" + string(os.PathSeparator) + "file_test"
	defer os.Remove(tmpFilePath)
	if _, err = os.Stat(tmpFilePath); err == nil {
		os.Remove(tmpFilePath)
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

	// Create File
	err = vfs.Touch(tmpFilePath)
	assert.Nil(t, err)

	fh, err := vfs.Open(tmpFilePath, TILEDB_VFS_WRITE)
	assert.Nil(t, err)

	bytes := []byte{0, 1, 2}
	err = vfs.Write(fh, bytes)
	assert.Nil(t, err)

	bytes2, err := vfs.Read(fh, 0, uint64(len(bytes)))
	assert.Nil(t, err)
	assert.EqualValues(t, bytes, bytes2)

	dirSize, err := vfs.DirSize(tmpPath)
	assert.Nil(t, err)
	assert.EqualValues(t, 3, dirSize)

	// Calculate destination file path
	dstTmpFilePath := os.TempDir() + string(os.PathSeparator) + "tiledb_test_vfs" + string(os.PathSeparator) + "file_test_copy"
	defer os.Remove(dstTmpFilePath)
	if _, err = os.Stat(dstTmpFilePath); err == nil {
		os.Remove(dstTmpFilePath)
	}

	// Copy file
	err = vfs.CopyFile(tmpFilePath, dstTmpFilePath)
	assert.Nil(t, err)
	_, err = os.Stat(dstTmpFilePath)
	assert.Nil(t, err)
	if err == nil {
		os.Remove(dstTmpFilePath)
	}

	// Remove File
	err = vfs.RemoveFile(tmpFilePath)
	assert.Nil(t, err)

	// Remove directory
	err = vfs.RemoveDir(tmpPath)
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

func TestVFSFH(t *testing.T) {
	config, err := NewConfig()
	assert.Nil(t, err)

	context, err := NewContext(config)
	assert.Nil(t, err)

	vfs, err := NewVFS(context, config)
	assert.Nil(t, err)

	tmpPath := os.TempDir() + string(os.PathSeparator) + "tiledb_test_vfs_fh"
	defer os.Remove(tmpPath)
	if _, err = os.Stat(tmpPath); err == nil {
		os.Remove(tmpPath)
	}

	tmpFilePath := os.TempDir() + string(os.PathSeparator) + "tiledb_test_vfs_fh" + string(os.PathSeparator) + "file_test"
	defer os.Remove(tmpFilePath)
	if _, err = os.Stat(tmpFilePath); err == nil {
		os.Remove(tmpFilePath)
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

	w, err := vfs.Open(tmpFilePath, TILEDB_VFS_WRITE)
	assert.Nil(t, err)
	b := []byte{1, 2, 3}
	writeN, err := w.Write(b)
	assert.Nil(t, err)
	assert.Equal(t, 3, writeN)
	err = w.Close()
	assert.Nil(t, err)

	r, err := vfs.Open(tmpFilePath, TILEDB_VFS_READ)
	assert.Nil(t, err)
	bRead := make([]byte, 3)
	n, err := r.Read(bRead)
	assert.Nil(t, err)
	assert.Equal(t, 3, n)
	assert.Nil(t, err)
	assert.ElementsMatch(t, b, bRead)

	n, err = r.Read(bRead)
	assert.NotNil(t, err)
	assert.Equal(t, io.EOF, err)
	assert.Equal(t, 0, n)

	noffset, err := r.Seek(0, io.SeekStart)
	assert.Nil(t, err)
	assert.EqualValues(t, 0, noffset)
	n, err = r.Read(bRead)
	assert.Nil(t, err)
	assert.Equal(t, 3, n)
	err = r.Close()
	assert.Nil(t, err)
	assert.ElementsMatch(t, b, bRead)
}
