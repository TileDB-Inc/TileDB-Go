package tiledb

/*
#include <tiledb/tiledb_experimental.h>
#include <tiledb/tiledb_serialization.h>
#include <stdlib.h>
*/
import "C"
import (
	"errors"
	"fmt"
	"io"
	"runtime"
	"unsafe"
)

// FileSize returns the uncompressed size of the array at arrayURI, which should have a filestore schema.
func FileSize(tdbCtx *Context, arrayURI string) (int64, error) {
	cArrayURI := C.CString(arrayURI)
	defer C.free(unsafe.Pointer(cArrayURI))

	var size C.size_t
	ret := C.tiledb_filestore_size(tdbCtx.tiledbContext, cArrayURI, &size)
	runtime.KeepAlive(tdbCtx)
	if ret != C.TILEDB_OK {
		return 0, fmt.Errorf("error getting file size: %w", tdbCtx.LastError())
	}

	return int64(size), nil
}

// ExportFile reads the contents of the array at arrayURI, which should have a filestore schema,
// and writes them to the local file at filePath. All the subdirectories of filePath must exist.
func ExportFile(tdbCtx *Context, filePath, arrayURI string) error {
	cArrayURI := C.CString(arrayURI)
	defer C.free(unsafe.Pointer(cArrayURI))
	cFileURI := C.CString(filePath)
	defer C.free(unsafe.Pointer(cFileURI))

	ret := C.tiledb_filestore_uri_export(tdbCtx.tiledbContext, cFileURI, cArrayURI)
	runtime.KeepAlive(tdbCtx)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("error exporting file: %w", tdbCtx.LastError())
	}

	return nil
}

// ImportFile stores the contents of the local file at filePath to the array at arrayURI, which should have a filestore schema.
func ImportFile(tdbCtx *Context, arrayURI, filePath string, mimeType FileStoreMimeType) error {
	cArrayURI := C.CString(arrayURI)
	defer C.free(unsafe.Pointer(cArrayURI))
	cFileURI := C.CString(filePath)
	defer C.free(unsafe.Pointer(cFileURI))

	ret := C.tiledb_filestore_uri_import(tdbCtx.tiledbContext, cArrayURI, cFileURI, C.tiledb_mime_type_t(mimeType))
	runtime.KeepAlive(tdbCtx)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("error importing file: %w", tdbCtx.LastError())
	}

	return nil
}

// CreateAndImportFile creates at arrayURI a TileDB array suitable to store the local file at filePath and imports the contents.
func CreateAndImportFile(tdbCtx *Context, arrayURI string, filePath string, mimeType FileStoreMimeType) error {
	schema, err := NewArraySchemaForFile(tdbCtx, filePath)
	if err != nil {
		return err
	}

	array, err := NewArray(tdbCtx, arrayURI)
	if err != nil {
		return err
	}

	err = array.Create(schema)
	if err != nil {
		return err
	}

	return ImportFile(tdbCtx, arrayURI, filePath, mimeType)
}

// CreateFile creates at arrayURI a TileDB array with the filestore schema and writes the data.
// The array is created even if data is empty.
func CreateFile(tdbCtx *Context, arrayURI string, data []byte, mimeType FileStoreMimeType) error {
	schema, err := NewArraySchemaForFile(tdbCtx, "")
	if err != nil {
		return err
	}

	array, err := NewArray(tdbCtx, arrayURI)
	if err != nil {
		return err
	}

	err = array.Create(schema)
	if err != nil {
		return err
	}

	if len(data) == 0 {
		return nil
	}

	cArrayURI := C.CString(arrayURI)
	defer C.free(unsafe.Pointer(cArrayURI))
	return bufferImport(tdbCtx, cArrayURI, data, mimeType)
}

// File represents a TileDB filestore file.
// This is a regular TileDB array, you can query and checkout older versions,
// and it has a schema suitable to store files as byte arrays.
type File struct {
	tdbCtx    *Context // the tiledb context for all operations
	arrayURI  string   // the uri of the array
	arraySize int64    // the size of the array as returned by FileSize
	bytesRead int64    // the total bytes read so far. Used as an offset for read operations
}

// Read satisfies io.Reader.
func (f *File) Read(p []byte) (n int, err error) {
	bytesRemaining := f.arraySize - f.bytesRead
	if bytesRemaining == 0 {
		return 0, io.EOF
	}
	if len(p) == 0 {
		return 0, nil
	}
	if int64(len(p)) > bytesRemaining {
		p = p[0:bytesRemaining]
	}

	cArrayURI := C.CString(f.arrayURI)
	defer C.free(unsafe.Pointer(cArrayURI))
	if err := bufferExport(f.tdbCtx, cArrayURI, f.bytesRead, p); err != nil {
		return 0, err
	}
	f.bytesRead += int64(len(p))

	return len(p), nil
}

// OpenFile opens for reading the array at arrayURI, which should have a filestore schema.
func OpenFile(tdbCtx *Context, arrayURI string) (*File, error) {
	siz, err := FileSize(tdbCtx, arrayURI)
	if err != nil {
		return nil, err
	}
	return &File{
		tdbCtx:    tdbCtx,
		arrayURI:  arrayURI,
		arraySize: siz,
	}, nil
}

// NewArraySchemaForFile allocates a new ArraySchema optimized for the storage of file.
// An empty path returns a general schema suitable for any file.
func NewArraySchemaForFile(tdbCtx *Context, filePath string) (*ArraySchema, error) {
	var fileURI *C.char
	if filePath != "" {
		fileURI = C.CString(filePath)
		defer C.free(unsafe.Pointer(fileURI))
	}

	arraySchema := ArraySchema{context: tdbCtx}
	ret := C.tiledb_filestore_schema_create(tdbCtx.tiledbContext, fileURI, &arraySchema.tiledbArraySchema)
	runtime.KeepAlive(tdbCtx)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("error creating schema: %w", tdbCtx.LastError())
	}
	runtime.AddCleanup(&arraySchema, freeFreeable, Freeable(&arraySchema))

	return &arraySchema, nil
}

// bufferExport reads len(p) bytes into p starting at array offset off
// It is an error to try to read more bytes than available. Use FileStore.Size to adjust queries.
// Uri is the uri of an existing array with the filestore schema (see ArraySchemaForFile)
func bufferExport(tdbCtx *Context, uri *C.char, off int64, p []byte) error {
	if len(p) == 0 {
		return nil
	}

	ret := C.tiledb_filestore_buffer_export(tdbCtx.tiledbContext, uri, C.size_t(off), slicePtr(p), C.size_t(len(p)))
	runtime.KeepAlive(tdbCtx)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("error exporting buffer data: %w", tdbCtx.LastError())
	}

	return nil
}

// bufferImport overwrites the contents of the filestore array with the contents of data
// Uri is the uri of an existing array with the filestore schema (see ArraySchemaForFile)
func bufferImport(tdbCtx *Context, uri *C.char, data []byte, mimeType FileStoreMimeType) error {
	if len(data) == 0 {
		return errors.New("error importing buffer data: empty data")
	}

	ret := C.tiledb_filestore_buffer_import(tdbCtx.tiledbContext, uri, slicePtr(data), C.size_t(len(data)), C.tiledb_mime_type_t(mimeType))
	runtime.KeepAlive(tdbCtx)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("error importing buffer data: %w", tdbCtx.LastError())
	}

	return nil
}
