package examples

import (
	"fmt"
	"github.com/TileDB-Inc/TileDB-Go"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

// Name of the group
var groupName = "my_group"

// Type of file system (e.g. file://, s3://)
var fileSystem = "file://"

func ExampleErrors() {
	// Get filename of current file

	// uncomment to use local filesystem
	_, filename, _, _ := runtime.Caller(0)
	pathName := filepath.Dir(filename)

	// uncomment to use s3 bucket
	//pathName := "test-bucket"

	// Construct the path
	groupPathName :=
		fmt.Sprintf("%s%s/%s", fileSystem, pathName, groupName)

	// Create config
	config, err := tiledb.NewConfig()
	checkError(err)

	// Create a TileDB context.
	ctx, err := tiledb.NewContext(config)
	checkError(err)

	// Create vfs
	vfs, err := tiledb.NewVFS(ctx, config)
	checkError(err)

	// Find out if dir exists having group name
	isDir, err := vfs.IsDir(groupName)
	checkError(err)

	// If it exists delete it to start clean
	if isDir {
		// For local filesystem it suffices to replace groupPathName with
		// groupName since vfs can infer local directory
		err = vfs.RemoveDir(groupPathName)
		checkError(err)
	}

	err = tiledb.GroupCreate(ctx, groupPathName)
	checkError(err)
	//There cannot be two groups having the same name
	err = tiledb.GroupCreate(ctx, groupPathName)

	if err != nil {
		if strings.Contains(err.Error(), "already exists") {
			fmt.Println("[TileDB::StorageManager] Error: Cannot create group")
			fmt.Println("Group already exists")
		}
	}

	// Clean up
	err = os.RemoveAll(groupName)
	checkError(err)

	// Output: [TileDB::StorageManager] Error: Cannot create group
	// Group already exists
}
