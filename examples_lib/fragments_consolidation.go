package examples_lib

import (
	"fmt"
	"os"

	tiledb "github.com/TileDB-Inc/TileDB-Go"
)

// Name of array.
var fragmentsConsolidationArrayName = "fragments_consolidation_array"

func createFragmentsConsolidationArray() {
	// Create a TileDB context.
	ctx, err := tiledb.NewContext(nil)
	checkError(err)
	defer ctx.Free()
	// The array will be 4x4 with dimensions "rows" and "cols", with domain [1,4].
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

	// The array will be dense.
	schema, err := tiledb.NewArraySchema(ctx, tiledb.TILEDB_DENSE)
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
	array, err := tiledb.NewArray(ctx, fragmentsConsolidationArrayName)
	checkError(err)
	defer array.Free()

	err = array.Create(schema)
	checkError(err)
}

func writeFragmentsConsolidationArray1() {
	ctx, err := tiledb.NewContext(nil)
	checkError(err)
	defer ctx.Free()

	// Prepare some data for the array
	data := []int32{1, 2, 3, 4, 5, 6, 7, 8}
	subarray := []int32{1, 2, 1, 4}

	// Create the query
	array, err := tiledb.NewArray(ctx, fragmentsConsolidationArrayName)
	checkError(err)
	defer array.Free()

	err = array.Open(tiledb.TILEDB_WRITE)
	checkError(err)
	defer array.Close()

	query, err := tiledb.NewQuery(ctx, array)
	checkError(err)
	defer query.Free()

	err = query.SetLayout(tiledb.TILEDB_ROW_MAJOR)
	checkError(err)
	_, err = query.SetBuffer("a", data)
	checkError(err)
	err = query.SetSubArray(subarray)
	checkError(err)

	// Perform the write and close the array.
	err = query.Submit()
	checkError(err)

	err = query.Finalize()
	checkError(err)
}

func writeFragmentsConsolidationArray2() {
	ctx, err := tiledb.NewContext(nil)
	checkError(err)
	defer ctx.Free()

	// Prepare some data for the array
	data := []int32{101, 102, 103, 104}
	subarray := []int32{2, 3, 2, 3}

	// Create the query
	array, err := tiledb.NewArray(ctx, fragmentsConsolidationArrayName)
	checkError(err)
	defer array.Free()

	err = array.Open(tiledb.TILEDB_WRITE)
	checkError(err)
	defer array.Close()

	query, err := tiledb.NewQuery(ctx, array)
	checkError(err)
	defer query.Free()

	err = query.SetLayout(tiledb.TILEDB_ROW_MAJOR)
	checkError(err)
	_, err = query.SetBuffer("a", data)
	checkError(err)
	err = query.SetSubArray(subarray)
	checkError(err)

	// Perform the write and close the array.
	err = query.Submit()
	checkError(err)

	err = query.Finalize()
	checkError(err)
}

func writeFragmentsConsolidationArray3() {
	ctx, err := tiledb.NewContext(nil)
	checkError(err)

	// Prepare some data for the array
	buffD1 := []int32{1, 3}
	buffD2 := []int32{1, 4}
	data := []int32{201, 202}

	// Create the query
	array, err := tiledb.NewArray(ctx, fragmentsConsolidationArrayName)
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
	_, err = query.SetBuffer("a", data)
	checkError(err)
	_, err = query.SetBuffer("rows", buffD1)
	checkError(err)
	_, err = query.SetBuffer("cols", buffD2)
	checkError(err)

	// Perform the write and close the array.
	err = query.Submit()
	checkError(err)

	num, err := query.GetFragmentNum()
	checkError(err)
	fmt.Printf("Num of fragments: %d\n", *num)

	_, err = query.GetFragmentURI(0)
	checkError(err)
	// fmt.Printf("Uri of fragment: %d is: %s\n", 0, *uri)

	_, _, err = query.GetFragmentTimestampRange(0)
	checkError(err)
	// fmt.Printf("Timestamp range for fragment: %d is t1: %d, t2: %d\n", 0, *t1, *t2)

	err = query.Finalize()
	checkError(err)
}

func readFragmentsConsolidationArray() {
	// Create TileDB context
	ctx, err := tiledb.NewContext(nil)
	checkError(err)
	defer ctx.Free()

	// Prepare the array for reading
	array, err := tiledb.NewArray(ctx, fragmentsConsolidationArrayName)
	checkError(err)
	defer array.Free()

	err = array.Open(tiledb.TILEDB_READ)
	checkError(err)
	defer array.Close()

	// Read the entire array
	subArray := []int32{1, 4, 1, 4}

	// Prepare the query
	query, err := tiledb.NewQuery(ctx, array)
	checkError(err)
	defer query.Free()

	err = query.SetSubArray(subArray)
	checkError(err)

	// Prepare the vector that will hold the result
	bufferElements, err := query.EstimateBufferElements()
	checkError(err)

	data := make([]int32, bufferElements["a"][1])
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

func RunFragmentsConsolidationArray() {
	createFragmentsConsolidationArray()
	writeFragmentsConsolidationArray1()
	writeFragmentsConsolidationArray2()
	writeFragmentsConsolidationArray3()
	readFragmentsConsolidationArray()

	// Cleanup example so unit tests are clean
	if _, err := os.Stat(fragmentsConsolidationArrayName); err == nil {
		err = os.RemoveAll(fragmentsConsolidationArrayName)
		checkError(err)
	}
}
