package examples_lib

import (
	"fmt"
	"os"

	tiledb "github.com/TileDB-Inc/TileDB-Go"
)

// Name of array.
const densePaddingArrayName = "writing_dense_padding_array"

func createDensePaddingArray() {
	// Create a TileDB context.
	ctx, err := tiledb.NewContext(nil)
	checkError(err)
	defer ctx.Free()

	// The array will be 4x4 with dimensions "rows" and "cols", with domain [1,4]
	// and space tiles 2x2
	domain, err := tiledb.NewDomain(ctx)
	checkError(err)
	defer domain.Free()

	rowDim, err := tiledb.NewDimension(ctx, "rows", tiledb.TILEDB_INT32, []int32{1, 4}, int32(2))
	checkError(err)
	defer rowDim.Free()

	colDim, err := tiledb.NewDimension(ctx, "cols", tiledb.TILEDB_INT32, []int32{1, 4}, int32(2))
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
	array, err := tiledb.NewArray(ctx, densePaddingArrayName)
	checkError(err)
	defer array.Free()

	err = array.Create(schema)
	checkError(err)
}

func writeDensePaddingArray() {
	ctx, err := tiledb.NewContext(nil)
	checkError(err)
	defer ctx.Free()

	subarray := []int32{2, 3, 1, 2}

	// Open the array for writing.
	array, err := tiledb.NewArray(ctx, densePaddingArrayName)
	checkError(err)
	defer array.Free()

	err = array.Open(tiledb.TILEDB_WRITE)
	checkError(err)
	defer array.Close()

	query, err := tiledb.NewQuery(ctx, array)
	checkError(err)
	defer query.Free()

	// First submission
	data := []int32{1, 2, 3, 4}
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

func readDensePaddingArray() {
	ctx, err := tiledb.NewContext(nil)
	checkError(err)
	defer ctx.Free()

	// Prepare the array for reading
	array, err := tiledb.NewArray(ctx, densePaddingArrayName)
	checkError(err)
	defer array.Free()

	err = array.Open(tiledb.TILEDB_READ)
	checkError(err)
	defer array.Close()

	// Read the entire array
	subArray := []int32{1, 4, 1, 4}

	// Prepare the vector that will hold the result (of size 16 elements)
	data := make([]int32, 16)

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

func RunWritingDensePadding() {
	createDensePaddingArray()
	writeDensePaddingArray()
	readDensePaddingArray()

	// Cleanup example so unit tests are clean
	if _, err := os.Stat(densePaddingArrayName); err == nil {
		err = os.RemoveAll(densePaddingArrayName)
		checkError(err)
	}
}
