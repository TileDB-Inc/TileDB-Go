package tiledb

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestVFS validates vfs file operations are successful
func TestVFS(t *testing.T) {
	config, err := NewConfig()
	require.NoError(t, err)

	context, err := NewContext(config)
	require.NoError(t, err)

	vfs, err := NewVFS(context, config)
	require.NoError(t, err)

	tmpPath := filepath.Join(t.TempDir(), "somedir")

	tmpFilePath := filepath.Join(tmpPath, "somefile")

	isFile, err := vfs.IsFile(tmpPath)
	require.NoError(t, err)
	assert.False(t, isFile)

	isDir, err := vfs.IsDir(tmpPath)
	require.NoError(t, err)
	assert.False(t, isDir)

	// Create directory
	require.NoError(t, vfs.CreateDir(tmpPath))

	isDir, err = vfs.IsDir(tmpPath)
	require.NoError(t, err)
	assert.True(t, isDir)

	// Create File
	require.NoError(t, vfs.Touch(tmpFilePath))

	fh, err := vfs.Open(tmpFilePath, TILEDB_VFS_WRITE)
	require.NoError(t, err)

	bytes := []byte{0, 1, 2}
	require.NoError(t, vfs.Write(fh, bytes))

	bytes2, err := vfs.Read(fh, 0, uint64(len(bytes)))
	require.NoError(t, err)
	assert.EqualValues(t, bytes, bytes2)

	dirSize, err := vfs.DirSize(tmpPath)
	require.NoError(t, err)
	assert.EqualValues(t, 3, dirSize)

	// Calculate destination file path
	dstTmpFilePath := filepath.Join(t.TempDir(), "copy-dest")

	// Copy file
	require.NoError(t, vfs.CopyFile(tmpFilePath, dstTmpFilePath))
	_, err = os.Stat(dstTmpFilePath)
	require.NoError(t, err)
	if err == nil {
		os.Remove(dstTmpFilePath)
	}

	// Remove File
	require.NoError(t, vfs.RemoveFile(tmpFilePath))

	// Remove directory
	require.NoError(t, vfs.RemoveDir(tmpPath))
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
	require.NoError(t, err)

	context, err := NewContext(config)
	require.NoError(t, err)

	vfs, err := NewVFS(context, config)
	require.NoError(t, err)

	tmpPath := filepath.Join(t.TempDir(), "somedir")

	tmpFilePath := filepath.Join(t.TempDir(), "somefile")

	isFile, err := vfs.IsFile(tmpPath)
	require.NoError(t, err)
	assert.False(t, isFile)

	isDir, err := vfs.IsDir(tmpPath)
	require.NoError(t, err)
	assert.False(t, isDir)

	// Create directory
	require.NoError(t, vfs.CreateDir(tmpPath))

	isDir, err = vfs.IsDir(tmpPath)
	require.NoError(t, err)
	assert.True(t, isDir)

	w, err := vfs.Open(tmpFilePath, TILEDB_VFS_WRITE)
	require.NoError(t, err)
	b := []byte{1, 2, 3}
	writeN, err := w.Write(b)
	require.NoError(t, err)
	assert.Equal(t, 3, writeN)
	require.NoError(t, w.Close())

	r, err := vfs.Open(tmpFilePath, TILEDB_VFS_READ)
	require.NoError(t, err)
	bRead := make([]byte, 3)
	n, err := r.Read(bRead)
	require.NoError(t, err)
	assert.Equal(t, 3, n)
	require.NoError(t, err)
	assert.ElementsMatch(t, b, bRead)

	n, err = r.Read(bRead)
	assert.Error(t, err)
	assert.Equal(t, io.EOF, err)
	assert.Equal(t, 0, n)

	noffset, err := r.Seek(0, io.SeekStart)
	require.NoError(t, err)
	assert.EqualValues(t, 0, noffset)
	n, err = r.Read(bRead)
	require.NoError(t, err)
	assert.Equal(t, 3, n)
	require.NoError(t, r.Close())
	assert.ElementsMatch(t, b, bRead)
}

// TestVFSLs validates vfs LsDir operation is successful
func TestVFSLsDir(t *testing.T) {
	config, err := NewConfig()
	require.NoError(t, err)

	context, err := NewContext(config)
	require.NoError(t, err)

	vfs, err := NewVFS(context, config)
	require.NoError(t, err)

	tmpPath := filepath.Join(t.TempDir(), "somedir")
	tmpPath2 := filepath.Join(tmpPath, "somedir2")
	tmpPath3 := filepath.Join(tmpPath, "somedir3")

	isDir, err := vfs.IsDir(tmpPath)
	require.NoError(t, err)
	assert.False(t, isDir)

	isDir, err = vfs.IsDir(tmpPath2)
	require.NoError(t, err)
	assert.False(t, isDir)

	isDir, err = vfs.IsDir(tmpPath3)
	require.NoError(t, err)
	assert.False(t, isDir)

	// Create directories
	require.NoError(t, vfs.CreateDir(tmpPath))
	require.NoError(t, vfs.CreateDir(tmpPath2))
	require.NoError(t, vfs.CreateDir(tmpPath3))

	isDir, err = vfs.IsDir(tmpPath)
	require.NoError(t, err)
	assert.True(t, isDir)

	isDir, err = vfs.IsDir(tmpPath2)
	require.NoError(t, err)
	assert.True(t, isDir)

	isDir, err = vfs.IsDir(tmpPath3)
	require.NoError(t, err)
	assert.True(t, isDir)

	dirList, err := vfs.LsDir(tmpPath)
	require.NoError(t, err)
	assert.Equal(t, 2, len(dirList))

	// Remove directories
	require.NoError(t, vfs.RemoveDir(tmpPath))
}
