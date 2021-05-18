package examples_lib

import (
	"fmt"
	"os"

	tiledb "github.com/TileDB-Inc/TileDB-Go"
)

// Name of array.
var asyncArrayName = "async_array"

func createAsyncArray() {
	// Create a TileDB context.
	ctx, err := tiledb.NewContext(nil)
	checkError(err)
	defer ctx.Free()

	// The array will be 4x4 with dimensions "rows" and "cols",
	// with domain [1,4].
	domain, err := tiledb.NewDomain(ctx)
	checkError(err)
	defer domain.Free()

	rowDim, err := tiledb.NewDimension(ctx, "rows", tiledb.TILEDB_INT32, []int32{1, 4},
		int32(2))
	checkError(err)
	defer rowDim.Free()

	colDim, err := tiledb.NewDimension(ctx, "cols", tiledb.TILEDB_INT32, []int32{1, 4},
		int32(2))
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

	// Add a single attribute
	a, err := tiledb.NewAttribute(ctx, "a", tiledb.TILEDB_UINT32)
	checkError(err)
	defer a.Free()

	err = schema.AddAttributes(a)
	checkError(err)

	// Create the (empty) array on disk.
	array, err := tiledb.NewArray(ctx, asyncArrayName)
	checkError(err)
	err = array.Create(schema)
	checkError(err)

	array.Free()
}

func writeAsyncArray() {
	ctx, err := tiledb.NewContext(nil)
	checkError(err)
	defer ctx.Free()

	// Write some simple data to cells (1, 1), (2, 1), (2, 2) and (4, 3).
	buffD1 := []int32{1, 2, 2, 4}
	buffD2 := []int32{1, 1, 2, 3}
	data := []uint32{1, 2, 3, 4}

	// Open the array for writing and create the query.
	array, err := tiledb.NewArray(ctx, asyncArrayName)
	checkError(err)
	defer array.Free()

	err = array.Open(tiledb.TILEDB_WRITE)
	checkError(err)
	defer array.Close()

	query, err := tiledb.NewQuery(ctx, array)
	checkError(err)
	defer query.Free()

	err = query.SetLayout(tiledb.TILEDB_GLOBAL_ORDER)
	checkError(err)
	_, err = query.SetBuffer("a", data)
	checkError(err)
	_, err = query.SetBuffer("rows", buffD1)
	checkError(err)
	_, err = query.SetBuffer("cols", buffD2)
	checkError(err)

	// Submit query asynchronously
	// Async submits do not block
	err = query.SubmitAsync()
	checkError(err)

	fmt.Println("Write query in progress")

	// Wait for status to return complete or to error
	// Loop while status is inprogress
	for status, err := query.Status(); status == tiledb.TILEDB_INPROGRESS &&
		err == nil; status,
		err = query.Status() {
		// Do something while query is running
	}

	fmt.Println("Callback: Write query completed")

	// Perform the write and close the array.
	err = query.Finalize()
	checkError(err)
}

func readAsyncArray() {
	ctx, err := tiledb.NewContext(nil)
	checkError(err)
	defer ctx.Free()

	// Prepare the array for reading
	array, err := tiledb.NewArray(ctx, asyncArrayName)
	checkError(err)
	defer array.Free()

	err = array.Open(tiledb.TILEDB_READ)
	checkError(err)
	defer array.Close()

	// Slice rows, cols 1, 2, 3, 4
	subArray := []int32{1, 4, 1, 4}

	// Prepare the query
	query, err := tiledb.NewQuery(ctx, array)
	checkError(err)
	defer query.Free()

	err = query.SetSubArray(subArray)
	checkError(err)

	// Prepare the vector that will hold the results
	bufferElements, err := query.EstimateBufferElements()
	checkError(err)
	data := make([]uint32, bufferElements["a"][1])
	rows := make([]int32, bufferElements["rows"][1])
	cols := make([]int32, bufferElements["cols"][1])

	err = query.SetLayout(tiledb.TILEDB_ROW_MAJOR)
	checkError(err)
	_, err = query.SetBuffer("a", data)
	checkError(err)
	_, err = query.SetBuffer("rows", rows)
	checkError(err)
	_, err = query.SetBuffer("cols", cols)
	checkError(err)

	// Submit query asynchronously
	// Async submits do not block
	err = query.SubmitAsync()
	checkError(err)

	fmt.Println("Read query in progress")

	// Wait for status to return complete or to error
	// Loop while status is inprogress
	for status, err := query.Status(); status == tiledb.TILEDB_INPROGRESS &&
		err == nil; status,
		err = query.Status() {
		// Do something while query is running
	}

	fmt.Println("Callback: Read query completed")

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
}

func RunAsyncArray() {
	createAsyncArray()
	writeAsyncArray()
	readAsyncArray()

	// Cleanup example so unit tests are clean
	if _, err := os.Stat(asyncArrayName); err == nil {
		err = os.RemoveAll(asyncArrayName)
		checkError(err)
	}
}
