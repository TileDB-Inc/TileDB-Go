//go:build experimental
// +build experimental

package tiledb

import (
	"bytes"
	"crypto/sha1"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestArraySchemaForFile(t *testing.T) {
	for _, filePath := range []string{"", "testdata/VLDB17_TileDB_Page1.pdf"} {
		config, err := NewConfig()
		require.NoError(t, err)
		context, err := NewContext(config)
		require.NoError(t, err)

		arraySchema, err := NewArraySchemaForFile(context, filePath)
		require.NoError(t, err)
		assert.NotNil(t, arraySchema)
		require.NoError(t, arraySchema.Check())

		arraySchema.Free()
	}
}

func TestCreateFile(t *testing.T) {
	t.Run("WithData", func(t *testing.T) {
		tempDir := t.TempDir()

		tdbCtx, err := NewContext(nil)
		require.NoError(t, err)

		// read a file data
		origFile := "testdata/VLDB17_TileDB_Page1.pdf"
		data, err := os.ReadFile(origFile)
		require.NoError(t, err)

		// and create a filestore file with the data
		arrayURI := "file://" + filepath.Join(tempDir, "array")
		err = CreateFile(tdbCtx, arrayURI, data, TILEDB_MIME_PDF)
		require.NoError(t, err)

		// check size
		n, err := FileSize(tdbCtx, arrayURI)
		assert.NoError(t, err)
		assert.Equal(t, int64(len(data)), n)

		// export the file and check is the same
		exportedFile := filepath.Join(tempDir, "exported")
		err = ExportFile(tdbCtx, exportedFile, arrayURI)
		require.NoError(t, err)

		assertSameContents(t, origFile, exportedFile)
	})

	t.Run("WithoutData", func(t *testing.T) {
		tempDir := t.TempDir()

		tdbCtx, err := NewContext(nil)
		require.NoError(t, err)

		// create a filestore file
		arrayURI := "file://" + filepath.Join(tempDir, "array")
		err = CreateFile(tdbCtx, arrayURI, nil, TILEDB_MIME_AUTODETECT)
		require.NoError(t, err)

		// import a file to check that the schema is correct
		err = ImportFile(tdbCtx, arrayURI, "testdata/VLDB17_TileDB_Page1.pdf", TILEDB_MIME_PDF)
		require.NoError(t, err)
	})
}

func TestCreateAndImportFile(t *testing.T) {
	tempDir := t.TempDir()

	tests := []struct {
		arrayURI    string
		filePath    string
		mimeType    FileStoreMimeType
		expectError bool
	}{
		{"file://" + filepath.Join(tempDir, "array1"), "testdata/file-not-exists", TILEDB_MIME_AUTODETECT, true},
		{"file://" + filepath.Join(tempDir, "array2"), "testdata/tiledb-logo.png", TILEDB_MIME_AUTODETECT, false},
		{"file://" + filepath.Join(tempDir, "array3"), "testdata/tiledb.txt", TILEDB_MIME_AUTODETECT, false},
		{"file://" + filepath.Join(tempDir, "array4"), "testdata/VLDB17_TileDB_Page1.pdf", TILEDB_MIME_PDF, false},
	}

	for _, test := range tests {
		tdbCtx, err := NewContext(nil)
		require.NoError(t, err)
		err = CreateAndImportFile(tdbCtx, test.arrayURI, test.filePath, test.mimeType)
		if test.expectError {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
			assertEqualArraySizeAndFileSize(t, test.arrayURI, test.filePath)
		}
	}
}

func TestImportFile(t *testing.T) {
	tempDir := t.TempDir()

	// create an empty file array
	fileArrayURI := "file://" + filepath.Join(tempDir, "array")
	createEmptyFilestoreArray(t, fileArrayURI)

	nonFileArrayURI := URIOfEmptyTestArray(t)

	tests := []struct {
		arrayURI    string
		filePath    string
		mimeType    FileStoreMimeType
		expectError bool
	}{
		{"file:///array/does/not/exist", "testdata/tiledb-logo.png", TILEDB_MIME_AUTODETECT, true},
		{nonFileArrayURI, "testdata/tiledb-logo.png", TILEDB_MIME_AUTODETECT, true},
		{fileArrayURI, "testdata/file-not-exists", TILEDB_MIME_AUTODETECT, true},
		{fileArrayURI, "testdata/tiledb-logo.png", TILEDB_MIME_AUTODETECT, false},
		{fileArrayURI, "testdata/tiledb.txt", TILEDB_MIME_AUTODETECT, false},
		{fileArrayURI, "testdata/VLDB17_TileDB_Page1.pdf", TILEDB_MIME_PDF, false},
	}

	for _, test := range tests {
		tdbCtx, err := NewContext(nil)
		require.NoError(t, err)
		err = ImportFile(tdbCtx, test.arrayURI, test.filePath, test.mimeType)
		if test.expectError {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
			assertEqualArraySizeAndFileSize(t, test.arrayURI, test.filePath)
		}
	}
}

func TestExportFile(t *testing.T) {
	tempDir := t.TempDir()

	// create the file array
	importedFile := "testdata/VLDB17_TileDB_Page1.pdf"
	fileArrayURI := "file://" + filepath.Join(tempDir, "array")
	createFilestoreArrayWithContents(t, fileArrayURI, importedFile, TILEDB_MIME_PDF)

	nonFileArrayURI := URIOfEmptyTestArray(t)

	existingFile := filepath.Join(tempDir, "existing")
	err := os.WriteFile(existingFile, []byte("hello world"), 0)
	require.NoError(t, err)

	tests := []struct {
		filePath    string
		arrayURI    string
		expectError bool
	}{
		{existingFile, fileArrayURI, false},
		{filepath.Join(tempDir, "exported1"), "file:///array/does/not/exist", true},
		{filepath.Join(tempDir, "exported2"), nonFileArrayURI, true},
		{filepath.Join(tempDir, "exported3"), fileArrayURI, false},
	}

	for _, test := range tests {
		tdbCtx, err := NewContext(nil)
		require.NoError(t, err)
		err = ExportFile(tdbCtx, test.filePath, test.arrayURI)
		if test.expectError {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
			assertSameContents(t, importedFile, test.filePath)
		}
	}
}

func TestFileReader(t *testing.T) {
	tempDir := t.TempDir()

	// create the file array
	importedFile := "testdata/VLDB17_TileDB_Page1.pdf"
	fileArrayURI := "file://" + filepath.Join(tempDir, "array")
	createFilestoreArrayWithContents(t, fileArrayURI, importedFile, TILEDB_MIME_PDF)

	// create a reader
	tdbCtx, err := NewContext(nil)
	require.NoError(t, err)
	r, err := OpenFile(tdbCtx, fileArrayURI)
	require.NoError(t, err)

	// export the data via the reader
	var sink bytes.Buffer
	_, err = io.Copy(&sink, r)
	require.NoError(t, err)

	// verify the exported and imported data are the same
	fileData, err := os.ReadFile(importedFile)
	require.NoError(t, err)
	require.Equal(t, sha1.New().Sum(fileData), sha1.New().Sum(sink.Bytes()))
}

func assertEqualArraySizeAndFileSize(t *testing.T, arrayURI, filePath string) {
	tdbCtx, err := NewContext(nil)
	require.NoError(t, err)
	arraySize, err := FileSize(tdbCtx, arrayURI)
	require.NoError(t, err)

	info, err := os.Stat(filePath)
	require.NoError(t, err)
	fileSize := info.Size()

	assert.Equal(t, fileSize, arraySize)
}

func createEmptyFilestoreArray(t *testing.T, arrayURI string) {
	tdbCtx, err := NewContext(nil)
	require.NoError(t, err)
	schema, err := NewArraySchemaForFile(tdbCtx, "")
	require.NoError(t, err)
	array, err := NewArray(tdbCtx, arrayURI)
	require.NoError(t, err)
	err = array.Create(schema)
	require.NoError(t, err)
}

func createFilestoreArrayWithContents(t *testing.T, arrayURI string, filePath string, mimeType FileStoreMimeType) {
	tdbCtx, err := NewContext(nil)
	require.NoError(t, err)
	err = CreateAndImportFile(tdbCtx, arrayURI, filePath, mimeType)
	require.NoError(t, err)
}

func assertSameContents(t *testing.T, fileURI, exportURI string) {
	fileData, err := os.ReadFile(fileURI)
	require.NoError(t, err)
	fileDataSha1 := sha1.New().Sum(fileData)
	exportedData, err := os.ReadFile(exportURI)
	require.NoError(t, err)
	exportedDataSha1 := sha1.New().Sum(exportedData)
	assert.Equal(t, fileDataSha1, exportedDataSha1)
}

func URIOfEmptyTestArray(t *testing.T) string {
	arr, err := newTestArray(t)
	require.NoError(t, err)
	uri, err := arr.URI()
	require.NoError(t, err)
	return uri
}
