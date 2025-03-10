package examples

import "github.com/TileDB-Inc/TileDB-Go/examples_lib"

func ExampleRunVfs() {
	examples_lib.RunVfs()

	// Output: Created 'dir_A'
	// Created empty file 'dir_A/file_A'
	// Size of file 'dir_A/file_A': 0
	// Moving file 'dir_A/file_A' to 'dir_A/file_B'
	// Deleting 'dir_A/file_B' and 'dir_A'
	// Binary read:
	// 153.1
	// abcdefghijkl
}
