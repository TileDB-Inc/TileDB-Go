package examples_lib

import (
	"fmt"

	tiledb "github.com/TileDB-Inc/TileDB-Go"
)

func createMultipleWritesSparseArray(dir string) {
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
	a, err := tiledb.NewAttribute(ctx, "a", tiledb.TILEDB_INT32)
	checkError(err)
	defer a.Free()

	err = schema.AddAttributes(a)
	checkError(err)

	// Create the (empty) array on disk.
	err = tiledb.CreateArray(ctx, dir, schema)
	checkError(err)
}

func execQueryUnordered(tdbCtx *tiledb.Context, array *tiledb.Array,
	data []int32, buffD1 []int32, buffD2 []int32) {
	query, err := tiledb.NewQuery(tdbCtx, array)
	checkError(err)
	defer query.Free()

	err = query.SetLayout(tiledb.TILEDB_UNORDERED)
	checkError(err)

	// Submit query
	_, err = query.SetDataBuffer("a", data)
	checkError(err)
	_, err = query.SetDataBuffer("rows", buffD1)
	checkError(err)
	_, err = query.SetDataBuffer("cols", buffD2)
	checkError(err)

	// Perform the write.
	err = query.Submit()
	checkError(err)

	// IMPORTANT!
	err = query.Finalize()
	checkError(err)
}

func writeMultipleWritesSparseArray(dir string) {
	ctx, err := tiledb.NewContext(nil)
	checkError(err)
	defer ctx.Free()

	// Open the array for writing.
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

	// First write
	buffD1 := []int32{1, 2, 2}
	buffD2 := []int32{1, 4, 3}
	data1 := []int32{1, 2, 3}
	execQueryUnordered(ctx, array, data1, buffD1, buffD2)

	buffD1 = []int32{4, 2}
	buffD2 = []int32{1, 2}
	data2 := []int32{4, 20}
	execQueryUnordered(ctx, array, data2, buffD1, buffD2)

	err = query.Finalize()
	checkError(err)
}

func readMultipleWritesSparseArray(dir string) {
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

	// Read the whole array
	subArray := []int32{1, 4, 1, 4}

	// Prepare the query
	query, err := tiledb.NewQuery(ctx, array)
	checkError(err)
	defer query.Free()

	// Prepare the subarray
	subarray, err := array.NewSubarray()
	checkError(err)
	defer subarray.Free()

	err = subarray.SetSubArray(subArray)
	checkError(err)
	err = query.SetSubarray(subarray)
	checkError(err)

	// Prepare the vector that will hold the results
	// We take the upper bound on the result size as we do not know how large
	// a buffer is needed since the array is sparse
	bufferElements, err := query.EstimateBufferElements()
	checkError(err)
	data := make([]int32, bufferElements["a"][1])
	rows := make([]int32, bufferElements["rows"][1])
	cols := make([]int32, bufferElements["cols"][1])

	err = query.SetLayout(tiledb.TILEDB_ROW_MAJOR)
	checkError(err)
	_, err = query.SetDataBuffer("a", data)
	checkError(err)
	_, err = query.SetDataBuffer("rows", rows)
	checkError(err)
	_, err = query.SetDataBuffer("cols", cols)
	checkError(err)

	// Submit the query and close the array.
	err = query.Submit()
	checkError(err)

	// Print out the results.
	elements, err := query.ResultBufferElements()
	checkError(err)
	resultNum := elements["a"][1]
	for r := 0; r < int(resultNum); r++ {
		i := rows[r]
		j := cols[r]
		a := data[r]
		fmt.Printf("Cell (%d, %d) has data %d\n", i, j, a)
	}

	err = query.Finalize()
	checkError(err)
}

func RunWritingSparseMultiple() {
	tmpDir := temp("writing_sparse_multiple_array")
	defer cleanup(tmpDir)

	createMultipleWritesSparseArray(tmpDir)
	writeMultipleWritesSparseArray(tmpDir)
	readMultipleWritesSparseArray(tmpDir)
}
