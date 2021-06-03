package examples_lib

import (
	"fmt"

	tiledb "github.com/TileDB-Inc/TileDB-Go"
	"github.com/TileDB-Inc/TileDB-Go/bytesizes"
)

func createVacuumSparseArray(dir string) {
	// Create a TileDB context.
	ctx, err := tiledb.NewContext(nil)
	checkError(err)
	defer ctx.Free()

	// The array will be 4x4 with dimensions "rows" and "cols",
	// with domain [1,4].
	domain, err := tiledb.NewDomain(ctx)
	checkError(err)
	defer domain.Free()

	dDim, err := tiledb.NewDimension(ctx, "d", tiledb.TILEDB_INT32, []int32{1, 4}, int32(4))
	checkError(err)
	defer dDim.Free()

	err = domain.AddDimensions(dDim)
	checkError(err)

	// The array will be sparse.
	schema, err := tiledb.NewArraySchema(ctx, tiledb.TILEDB_SPARSE)
	checkError(err)
	defer schema.Free()

	err = schema.SetDomain(domain)
	checkError(err)

	// Add a single attribute "a" so each (i) cell can store an integer.
	a, err := tiledb.NewAttribute(ctx, "a", tiledb.TILEDB_INT32)
	checkError(err)
	defer a.Free()

	err = schema.AddAttributes(a)
	checkError(err)

	// Create the (empty) array on disk.
	array, err := tiledb.NewArray(ctx, dir)
	checkError(err)
	defer array.Free()

	err = array.Create(schema)
	checkError(err)
}

func writeVacuumSparseArray(dir string, buffD []int32, data []int32) {
	ctx, err := tiledb.NewContext(nil)
	checkError(err)
	defer ctx.Free()

	// Open the array for writing and create the query.
	array, err := tiledb.NewArray(ctx, dir)
	checkError(err)
	defer array.Free()

	err = array.Open(tiledb.TILEDB_WRITE)
	checkError(err)
	defer array.Close()

	query, err := tiledb.NewQuery(ctx, array)
	checkError(err)
	defer query.Free()

	err = query.SetLayout(tiledb.TILEDB_UNORDERED)
	checkError(err)
	_, err = query.SetBuffer("d", buffD)
	checkError(err)
	_, err = query.SetBuffer("a", data)
	checkError(err)

	// Perform the write
	err = query.Submit()
	checkError(err)

	err = query.Finalize()
	checkError(err)
}

func readVacuumSparseArray(dir string) {
	ctx, err := tiledb.NewContext(nil)
	checkError(err)
	defer ctx.Free()

	// Prepare the array for reading
	array, err := tiledb.NewArray(ctx, dir)
	checkError(err)
	defer array.Free()

	err = array.Open(tiledb.TILEDB_READ)
	checkError(err)
	defer array.Close()

	// Prepare the query
	query, err := tiledb.NewQuery(ctx, array)
	checkError(err)
	defer query.Free()

	err = query.SetLayout(tiledb.TILEDB_UNORDERED)
	checkError(err)
	err = query.AddRange(0, int32(1), int32(3))
	checkError(err)

	size, err := query.EstResultSize("a")
	checkError(err)
	fmt.Printf("Estimated query size in bytes for attribute 'a': %d\n", *size)
	buffA := make([]int32, (*size)/bytesizes.Int32)

	size, err = query.EstResultSize("d")
	checkError(err)
	fmt.Printf("Estimated query size in bytes for dimension 'd': %d\n", *size)
	buffD := make([]int32, (*size)/bytesizes.Int32)

	_, err = query.SetBuffer("d", buffD)
	checkError(err)
	_, err = query.SetBuffer("a", buffA)
	checkError(err)

	// Submit the query
	err = query.Submit()
	checkError(err)

	for i, aVal := range buffA {
		fmt.Printf("Cell (%d) has data %d\n", buffD[i], aVal)
	}

	err = query.Finalize()
	checkError(err)
}

func numFragments(dir string) int {
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

	num, err := vfs.NumOfFragmentsInPath(dir)
	checkError(err)

	return num
}

func consolidateVacuum(dir string) {
	// Write some simple data to cells (1, 2)
	buffD := []int32{1, 2}
	data := []int32{1, 2}
	writeVacuumSparseArray(dir, buffD, data)

	// Write some simple data to cell (3)
	buffD = []int32{3}
	data = []int32{3}
	writeVacuumSparseArray(dir, buffD, data)

	readVacuumSparseArray(dir)

	ctx, err := tiledb.NewContext(nil)
	checkError(err)

	// Prepare the array for reading
	array, err := tiledb.NewArray(ctx, dir)
	checkError(err)
	defer array.Free()

	err = array.Open(tiledb.TILEDB_WRITE)
	checkError(err)
	defer array.Close()

	numOfFragments := numFragments(dir)
	fmt.Printf("Num of fragments after 2 writes before consolidate: %d\n", numOfFragments)

	config, err := tiledb.NewConfig()
	checkError(err)

	err = config.Set("sm.consolidation.buffer_size", "4")
	checkError(err)

	err = array.Consolidate(config)
	checkError(err)

	numOfFragments = numFragments(dir)
	fmt.Printf("Num of fragments after consolidate: %d\n", numOfFragments)

	err = array.Vacuum(config)
	checkError(err)

	numOfFragments = numFragments(dir)
	fmt.Printf("Num of fragments after vacuum: %d\n", numOfFragments)

	readVacuumSparseArray(dir)
}

// RunVacuumSparseArray shows ysage of array vacuum function
func RunVacuumSparseArray() {
	tmpDir := temp("vacuum_sparse_array")
	defer cleanup(tmpDir)

	createVacuumSparseArray(tmpDir)
	consolidateVacuum(tmpDir)
}
