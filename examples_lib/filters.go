package examples_lib

import (
	"fmt"

	tiledb "github.com/TileDB-Inc/TileDB-Go"
)

func createFilterArray(dir string) {
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

	// Create two fixed-length attributes "a1" and "a2"
	a1, err := tiledb.NewAttribute(ctx, "a1", tiledb.TILEDB_UINT32)
	checkError(err)
	defer a1.Free()

	a2, err := tiledb.NewAttribute(ctx, "a2", tiledb.TILEDB_INT32)
	checkError(err)
	defer a2.Free()

	// a1 will be filtered by bit width reduction followed by zstd
	// compression.
	bitWidthReduction, err := tiledb.NewFilter(ctx,
		tiledb.TILEDB_FILTER_BIT_WIDTH_REDUCTION)
	checkError(err)
	defer bitWidthReduction.Free()

	compressionZstd, err := tiledb.NewFilter(ctx, tiledb.TILEDB_FILTER_ZSTD)
	checkError(err)
	defer compressionZstd.Free()

	a1Filters, err := tiledb.NewFilterList(ctx)
	checkError(err)
	defer a1Filters.Free()

	err = a1Filters.AddFilter(bitWidthReduction)
	checkError(err)
	err = a1Filters.AddFilter(compressionZstd)
	checkError(err)
	err = a1.SetFilterList(a1Filters)
	checkError(err)

	// a2 will just have a single gzip compression filter.
	compressionGzip, err := tiledb.NewFilter(ctx, tiledb.TILEDB_FILTER_GZIP)
	checkError(err)
	defer compressionGzip.Free()

	a2Filters, err := tiledb.NewFilterList(ctx)
	checkError(err)
	defer a2Filters.Free()

	err = a2Filters.AddFilter(compressionGzip)
	checkError(err)
	err = a2.SetFilterList(a2Filters)
	checkError(err)

	// Add the attributes
	err = schema.AddAttributes(a1)
	checkError(err)
	err = schema.AddAttributes(a2)
	checkError(err)

	// Create the (empty) array on disk.
	array, err := tiledb.NewArray(ctx, dir)
	checkError(err)
	defer array.Free()

	err = array.Create(schema)
	checkError(err)
}

func writeFiltersArray(dir string) {
	ctx, err := tiledb.NewContext(nil)
	checkError(err)
	defer ctx.Free()

	// Write some simple data to cells (1, 1), (2, 4) and (2, 3).
	buffD1 := []int32{1, 2, 2}
	buffD2 := []int32{1, 4, 3}
	dataA1 := []uint32{1, 2, 3}
	dataA2 := []int32{-1, -2, -3}

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
	_, err = query.SetDataBuffer("a1", dataA1)
	checkError(err)
	_, err = query.SetDataBuffer("a2", dataA2)
	checkError(err)
	_, err = query.SetDataBuffer("rows", buffD1)
	checkError(err)
	_, err = query.SetDataBuffer("cols", buffD2)
	checkError(err)

	// Perform the write and close the array.
	err = query.Submit()
	checkError(err)

	err = query.Finalize()
	checkError(err)
}

func readFiltersArray(dir string) {
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

	// Slice only rows 1, 2 and cols 2, 3, 4
	subarray, err := array.NewSubarray()
	checkError(err)
	defer subarray.Free()
	subarray.SetSubArray([]int32{1, 2, 2, 4})

	// Prepare the query
	query, err := tiledb.NewQuery(ctx, array)
	checkError(err)
	defer query.Free()

	err = query.SetSubarray(subarray)
	checkError(err)

	// Prepare the vector that will hold the results
	// We take the upper bound on the result size as we do not know how large
	// a buffer is needed since the array is sparse
	bufferElements, err := query.EstimateBufferElements()
	checkError(err)
	data := make([]uint32, bufferElements["a1"][1])
	rows := make([]int32, bufferElements["rows"][1])
	cols := make([]int32, bufferElements["cols"][1])

	err = query.SetLayout(tiledb.TILEDB_ROW_MAJOR)
	checkError(err)
	_, err = query.SetDataBuffer("a1", data)
	checkError(err)
	_, err = query.SetDataBuffer("rows", rows)
	checkError(err)
	_, err = query.SetDataBuffer("cols", cols)
	checkError(err)

	// Submit the query and close the array.
	err = query.Submit()
	checkError(err)

	err = query.Finalize()
	checkError(err)

	// Print out the results.
	elements, err := query.ResultBufferElements()
	checkError(err)
	resultNum := elements["a1"][1]
	for r := 0; r < int(resultNum); r++ {
		i := rows[r]
		j := cols[r]
		a := data[r]
		fmt.Printf("Cell (%d, %d) has data %d\n", i, j, a)
	}
}

// RunFiltersArray shows and example creation, writing and reading of a
// sparse array
func RunFiltersArray() {
	tmpDir := temp("filters_array")
	defer cleanup(tmpDir)

	createFilterArray(tmpDir)
	writeFiltersArray(tmpDir)
	readFiltersArray(tmpDir)
}
