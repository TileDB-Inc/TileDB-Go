package examples_lib

import (
	"fmt"
	"os"

	tiledb "github.com/TileDB-Inc/TileDB-Go"
	"github.com/TileDB-Inc/TileDB-Go/array_wrapper"
)

// Name of array.
var denseArrayName = "quickstart_dense"

func createDenseArray() {
	dimMap := make(map[string]array_wrapper.DimensionDetail)
	dimMap["rows"] = array_wrapper.DimensionDetail{
		Domain: []int32{1, 4},
		Extent: int32(4),
	}
	dimMap["cols"] = array_wrapper.DimensionDetail{
		Domain: []int32{1, 4},
		Extent: int32(4),
	}

	attrMap := make(map[string]array_wrapper.AttributeDetail)
	attrMap["a"] = array_wrapper.AttributeDetail{
		Datatype: tiledb.TILEDB_INT32,
	}

	// Create the (empty) array on disk.
	_, err := array_wrapper.NewDenseArray(denseArrayName,
		tiledb.TILEDB_ROW_MAJOR, tiledb.TILEDB_ROW_MAJOR, dimMap, attrMap)
	checkError(err)
}

func writeDenseArray() {
	ctx, err := tiledb.NewContext(nil)
	checkError(err)
	defer ctx.Free()

	// Prepare some data for the array
	data := []int32{
		1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}

	// Open the array for writing and create the query.
	array, err := tiledb.NewArray(ctx, denseArrayName)
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

	// Perform the write and close the array.
	err = query.Submit()
	checkError(err)

	err = query.Finalize()
	checkError(err)
}

func readDenseArray() {
	ctx, err := tiledb.NewContext(nil)
	checkError(err)
	defer ctx.Free()

	// Prepare the array for reading
	array, err := tiledb.NewArray(ctx, denseArrayName)
	checkError(err)
	defer array.Free()

	err = array.Open(tiledb.TILEDB_READ)
	checkError(err)
	defer array.Close()

	// Slice only rows 1, 2 and cols 2, 3, 4
	subArray := []int32{1, 2, 2, 4}

	// Prepare the vector that will hold the result (of size 6 elements)
	data := make([]int32, 6)

	// Prepare the query
	query, err := tiledb.NewQuery(ctx, array)
	checkError(err)
	defer query.Free()

	err = query.SetSubArray(subArray)
	checkError(err)
	err = query.SetLayout(tiledb.TILEDB_ROW_MAJOR)
	checkError(err)
	_, err = query.SetBuffer("a", data)
	checkError(err)

	// Submit the query and close the array.
	err = query.Submit()
	checkError(err)

	err = query.Finalize()
	checkError(err)

	// Print out the results.
	fmt.Println(data)
}

func RunDenseArray() {
	createDenseArray()
	writeDenseArray()
	readDenseArray()

	// Cleanup example so unit tests are clean
	if _, err := os.Stat(denseArrayName); err == nil {
		err = os.RemoveAll(denseArrayName)
		checkError(err)
	}
}
