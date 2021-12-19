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
	createFile(t, vfs, tmpFilePath)

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
func TestVFSLs(t *testing.T) {
	config, err := NewConfig()
	require.NoError(t, err)

	context, err := NewContext(config)
	require.NoError(t, err)

	vfs, err := NewVFS(context, config)
	require.NoError(t, err)

	tmpPath := filepath.Join(t.TempDir(), "somedir")
	tmpPath2 := filepath.Join(tmpPath, "subdir")
	tmpPath3 := filepath.Join(tmpPath, "subdir2")

	isDir, err := vfs.IsDir(tmpPath)
	require.NoError(t, err)
	assert.False(t, isDir)

	isDir, err = vfs.IsDir(tmpPath3)
	require.NoError(t, err)
	assert.False(t, isDir)

	isDir, err = vfs.IsDir(tmpPath3)
	require.NoError(t, err)
	assert.False(t, isDir)

	tmpFilePath := filepath.Join(tmpPath, "somefile")
	tmpFilePath2 := filepath.Join(tmpPath, "somefile2")
	tmpFilePath3 := filepath.Join(tmpPath, "somefile3")

	isFile, err := vfs.IsFile(tmpFilePath)
	require.NoError(t, err)
	assert.False(t, isFile)

	isFile, err = vfs.IsFile(tmpFilePath2)
	require.NoError(t, err)
	assert.False(t, isFile)

	isFile, err = vfs.IsFile(tmpFilePath3)
	require.NoError(t, err)
	assert.False(t, isFile)

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

	// Create Files
	createFile(t, vfs, tmpFilePath)
	createFile(t, vfs, tmpFilePath2)
	createFile(t, vfs, tmpFilePath3)

	isFile, err = vfs.IsFile(tmpFilePath)
	require.NoError(t, err)
	assert.True(t, isFile)

	isFile, err = vfs.IsFile(tmpFilePath2)
	require.NoError(t, err)
	assert.True(t, isFile)

	isFile, err = vfs.IsFile(tmpFilePath3)
	require.NoError(t, err)
	assert.True(t, isFile)

	folderList, fileList, err := vfs.List(tmpPath)
	require.NoError(t, err)
	assert.EqualValues(t, []string{"file://" + tmpPath2,
		"file://" + tmpPath3}, folderList)
	assert.EqualValues(t, []string{"file://" + tmpFilePath, "file://" +
		tmpFilePath2, "file://" + tmpFilePath3}, fileList)
}

func createFile(t testing.TB, vfs *VFS, path string) {
	t.Helper()
	require.NoError(t, vfs.Touch(path))

	fh, err := vfs.Open(path, TILEDB_VFS_WRITE)
	require.NoError(t, err)

	inBytes := []byte{0, 1, 2}
	require.NoError(t, vfs.Write(fh, inBytes))

	outBytes, err := vfs.Read(fh, 0, uint64(len(inBytes)))
	require.NoError(t, err)
	assert.EqualValues(t, inBytes, outBytes)
}
