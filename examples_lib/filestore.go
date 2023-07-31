//go:build experimental

package examples_lib

import (
	"io"
	"os"
	"path/filepath"

	tiledb "github.com/TileDB-Inc/TileDB-Go"
)

func RunFilestore() {
	tmpDir := temp("Filestore")
	defer cleanup(tmpDir)

	tdbCtx, err := tiledb.NewContext(nil)
	checkError(err)

	// create a TileDB file from data.
	err = tiledb.CreateFile(tdbCtx, filepath.Join(tmpDir, "hello"), []byte("Hello World"), tiledb.TILEDB_MIME_AUTODETECT)
	checkError(err)

	// export the contents of a TileDB file to a local file
	err = tiledb.ExportFile(tdbCtx, filepath.Join(tmpDir, "hello.txt"), filepath.Join(tmpDir, "hello"))
	checkError(err)

	// create an empty TileDB file and import data from a local file
	err = tiledb.CreateFile(tdbCtx, filepath.Join(tmpDir, "hello2"), nil, tiledb.TILEDB_MIME_AUTODETECT)
	checkError(err)
	err = tiledb.ImportFile(tdbCtx, filepath.Join(tmpDir, "hello2"), filepath.Join(tmpDir, "hello.txt"), tiledb.TILEDB_MIME_AUTODETECT)
	checkError(err)

	// you can create and import a file in one step.
	err = tiledb.CreateAndImportFile(tdbCtx, filepath.Join(tmpDir, "hello3"), filepath.Join(tmpDir, "hello.txt"), tiledb.TILEDB_MIME_AUTODETECT)
	checkError(err)

	// use a File as an io.Reader
	fin, err := tiledb.OpenFile(tdbCtx, filepath.Join(tmpDir, "hello"))
	checkError(err)

	_, err = io.Copy(os.Stdout, fin)
	checkError(err)
}
