package examples

import (
	"github.com/TileDB-Inc/TileDB-Go/examples_lib"
)

func ExampleErrors() {
	examples_lib.RunErrors()

	// Output: [TileDB::StorageManager] Error: Cannot create group
	// Group already exists
}
