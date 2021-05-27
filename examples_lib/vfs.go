package examples_lib

import (
	"encoding/binary"
	"fmt"
	"math"
	"os"

	tiledb "github.com/TileDB-Inc/TileDB-Go"
	"github.com/TileDB-Inc/TileDB-Go/bytesizes"
)

const vfsFileName = "tiledb_vfs.bin"

func dirsFiles() {
	// Create a TileDB context.
	ctx, err := tiledb.NewContext(nil)
	checkError(err)
	defer ctx.Free()

	// Create config object
	config, err := tiledb.NewConfig()
	checkError(err)
	defer config.Free()

	// Create TileDB VFS.
	vfs, err := tiledb.NewVFS(ctx, config)
	checkError(err)
	defer vfs.Free()

	isDir, err := vfs.IsDir("dir_A")
	checkError(err)

	if !isDir {
		err = vfs.CreateDir("dir_A")
		checkError(err)
		fmt.Println("Created 'dir_A'")
	} else {
		fmt.Println("'dir_A' already exists")
	}

	// Creating an (empty) file
	isFile, err := vfs.IsFile("dir_A/file_A")
	checkError(err)

	if !isFile {
		err = vfs.Touch("dir_A/file_A")
		checkError(err)
		fmt.Println("Created empty file 'dir_A/file_A'")
	} else {
		fmt.Println("'dir_A/file_A' already exists")
	}

	// Getting the file size
	fileSize, err := vfs.FileSize("dir_A/file_A")
	checkError(err)
	fmt.Printf("Size of file 'dir_A/file_A': %d\n", fileSize)

	// Moving files (moving directories is similar)
	fmt.Println("Moving file 'dir_A/file_A' to 'dir_A/file_B'")
	err = vfs.MoveFile("dir_A/file_A", "dir_A/file_B")
	checkError(err)

	// Deleting files and directories
	fmt.Println("Deleting 'dir_A/file_B' and 'dir_A'")
	err = vfs.RemoveFile("dir_A/file_B")
	checkError(err)
	err = vfs.RemoveDir("dir_A")
	checkError(err)
}

func float32FromBytes(bytes []byte) float32 {
	bits := binary.LittleEndian.Uint32(bytes)
	float := math.Float32frombits(bits)
	return float
}

func float32ToBytes(float float32) []byte {
	bits := math.Float32bits(float)
	bytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(bytes, bits)
	return bytes
}

func write() {
	// Create TileDB context
	ctx, err := tiledb.NewContext(nil)
	checkError(err)
	defer ctx.Free()

	// Create config object
	config, err := tiledb.NewConfig()
	checkError(err)
	defer config.Free()

	// Create TileDB VFS.
	vfs, err := tiledb.NewVFS(ctx, config)
	checkError(err)
	defer vfs.Free()

	// Write binary data
	fh1, err := vfs.Open(vfsFileName, tiledb.TILEDB_VFS_WRITE)
	defer vfs.Close(fh1)
	if err != nil {
		fmt.Printf("Error opening file '%s'\n", vfsFileName)
	}

	var f1 float32 = 153.0
	s1 := "abcd"
	err = vfs.Write(fh1, float32ToBytes(f1))
	checkError(err)
	err = vfs.Write(fh1, []byte(s1))
	checkError(err)

	// Write binary data again - this will overwrite the previous file
	fh2, err := vfs.Open("tiledb_vfs.bin", tiledb.TILEDB_VFS_WRITE)
	defer vfs.Close(fh2)
	if err != nil {
		fmt.Printf("Error opening file '%s' for write.\n", vfsFileName)
	}

	var f2 float32 = 153.1
	s2 := "abcdef"
	err = vfs.Write(fh2, float32ToBytes(f2))
	checkError(err)
	err = vfs.Write(fh2, []byte(s2))
	checkError(err)

	// Append binary data to existing file (this will NOT work on S3)
	fh3, err := vfs.Open("tiledb_vfs.bin", tiledb.TILEDB_VFS_APPEND)
	defer vfs.Close(fh3)
	if err != nil {
		fmt.Printf("Error opening file '%s' for append.\n", vfsFileName)
	}

	s3 := "ghijkl"
	err = vfs.Write(fh3, []byte(s3))
	checkError(err)
}

func read() {
	// Create TileDB context
	ctx, err := tiledb.NewContext(nil)
	checkError(err)
	defer ctx.Free()

	// Create config object
	config, err := tiledb.NewConfig()
	checkError(err)
	defer config.Free()

	// Create TileDB VFS.
	vfs, err := tiledb.NewVFS(ctx, config)
	checkError(err)
	defer vfs.Free()

	// Read binary data
	fh, err := vfs.Open("tiledb_vfs.bin", tiledb.TILEDB_VFS_READ)
	defer vfs.Close(fh)
	if err != nil {
		fmt.Printf("Error opening file '%s'\n", vfsFileName)
	}

	sizefFile, err := vfs.FileSize(vfsFileName)
	checkError(err)

	f1, err := vfs.Read(fh, 0, bytesizes.Float32)
	checkError(err)
	s1, err := vfs.Read(fh, bytesizes.Float32, sizefFile-bytesizes.Float32)
	checkError(err)

	fmt.Println("Binary read:")
	fmt.Println(float32FromBytes(f1))
	fmt.Println(string(s1))

	// Clean up
	err = os.RemoveAll(vfsFileName)
	checkError(err)
}

func RunVfs() {
	dirsFiles()
	write()
	read()
}
