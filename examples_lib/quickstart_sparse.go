package examples_lib

import (
	"fmt"

	tiledb "github.com/TileDB-Inc/TileDB-Go"
	"github.com/TileDB-Inc/TileDB-Go/bytesizes"
)

func createSparseArray(dir string) {
	// Create a TileDB context.
	ctx, err := tiledb.NewContext(nil)
	checkError(err)
	defer ctx.Free()

	// The array will be 4x4 with dimensions "rows" and "cols",
	// with domain [1,4].
	domain, err := tiledb.NewDomain(ctx)
	checkError(err)
	defer domain.Free()

	rowDim, err := tiledb.NewDimension(ctx, "rows", tiledb.TILEDB_INT32, []int32{1, 4}, int32(4))
	checkError(err)
	defer rowDim.Free()

	colDim, err := tiledb.NewDimension(ctx, "cols", tiledb.TILEDB_INT32, []int32{1, 4}, int32(4))
	checkError(err)
	defer colDim.Free()

	err = domain.AddDimensions(rowDim, colDim)
	checkError(err)

	// The array will be sparse.
	schema, err := tiledb.NewArraySchema(ctx, tiledb.TILEDB_SPARSE)
	checkError(err)
	defer schema.Free()

	err = schema.SetDomain(domain)
	checkError(err)
	err = schema.SetCellOrder(tiledb.TILEDB_ROW_MAJOR)
	checkError(err)
	err = schema.SetTileOrder(tiledb.TILEDB_ROW_MAJOR)
	checkError(err)

	// Add a single attribute "a" so each (i,j) cell can store an integer.
	a, err := tiledb.NewAttribute(ctx, "a", tiledb.TILEDB_UINT32)
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

func writeSparseArray(dir string) {
	ctx, err := tiledb.NewContext(nil)
	checkError(err)
	defer ctx.Free()

	// Write some simple data to cells (1, 1), (2, 4) and (2, 3).
	buffD1 := []int32{1, 2, 2}
	buffD2 := []int32{1, 4, 3}
	data := []uint32{1, 2, 3}

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
	_, err = query.SetBuffer("rows", buffD1)
	checkError(err)
	_, err = query.SetBuffer("cols", buffD2)
	checkError(err)
	_, err = query.SetBuffer("a", data)
	checkError(err)

	// Perform the write and close the array.
	err = query.Submit()
	checkError(err)

	err = query.Finalize()
	checkError(err)
}

func readSparseArray(dir string) {
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
	err = query.AddRange(0, int32(1), int32(2))
	checkError(err)
	err = query.AddRange(1, int32(1), int32(4))
	checkError(err)

	size, err := query.EstResultSize("a")
	checkError(err)
	fmt.Printf("Estimated query size in bytes for attribute 'a': %d\n", *size)
	buffAR := make([]uint32, (*size)/bytesizes.Int32)

	size, err = query.EstResultSize("rows")
	checkError(err)
	fmt.Printf("Estimated query size in bytes for dimension 'rows': %d\n", *size)
	buffD1R := make([]int32, (*size)/bytesizes.Int32)

	size, err = query.EstResultSize("cols")
	checkError(err)
	fmt.Printf("Estimated query size in bytes for dimension 'cols': %d\n", *size)
	buffD2R := make([]int32, (*size)/bytesizes.Int32)

	_, err = query.SetBuffer("rows", buffD1R)
	checkError(err)
	_, err = query.SetBuffer("cols", buffD2R)
	checkError(err)
	_, err = query.SetBuffer("a", buffAR)
	checkError(err)

	// Submit the query and close the array.
	err = query.Submit()
	checkError(err)

	for i, aVal := range buffAR {
		fmt.Printf("Cell (%d, %d) has data %d\n", buffD1R[i], buffD2R[i], aVal)
	}

	err = query.Finalize()
	checkError(err)
}

// RunSparseArray shows and example creation, writing and reading of a
// sparse array
func RunSparseArray() {
	tmpDir := temp("sparse_array")
	defer cleanup(tmpDir)

	createSparseArray(tmpDir)
	writeSparseArray(tmpDir)
	readSparseArray(tmpDir)
}
