package examples_lib

import (
	"fmt"
	"os"

	tiledb "github.com/TileDB-Inc/TileDB-Go"
)

// Name of array.
var rangeArrayName = "range_array"

func createRangeArray() {
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
	array, err := tiledb.NewArray(ctx, rangeArrayName)
	checkError(err)
	defer array.Free()

	err = array.Create(schema)
	checkError(err)
}

func writeRangeArray() {
	ctx, err := tiledb.NewContext(nil)
	checkError(err)
	defer ctx.Free()

	// Prepare some data for the array
	data := []int32{
		1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}

	// Open the array for writing and create the query.
	array, err := tiledb.NewArray(ctx, rangeArrayName)
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

func addRange() {
	ctx, err := tiledb.NewContext(nil)
	checkError(err)
	defer ctx.Free()

	// Prepare the array for reading
	array, err := tiledb.NewArray(ctx, rangeArrayName)
	checkError(err)
	defer array.Free()

	err = array.Open(tiledb.TILEDB_READ)
	checkError(err)
	defer array.Close()

	// Prepare the query
	query, err := tiledb.NewQuery(ctx, array)
	checkError(err)
	defer query.Free()

	// Try with invalid dimension types
	err = query.AddRange(0, float32(1), float32(3))
	fmt.Println(err)

	// Try with invalid dimension index
	err = query.AddRange(2, int32(1), int32(3))
	fmt.Println(err)

	// Try using valid index, range
	err = query.AddRange(0, int32(1), int32(3))
	checkError(err)

	err = query.Finalize()
	checkError(err)
}

func getRangeNum() {
	ctx, err := tiledb.NewContext(nil)
	checkError(err)

	// Prepare the array for reading
	array, err := tiledb.NewArray(ctx, rangeArrayName)
	checkError(err)
	defer array.Free()

	err = array.Open(tiledb.TILEDB_READ)
	checkError(err)
	defer array.Close()

	// Prepare the query
	query, err := tiledb.NewQuery(ctx, array)
	checkError(err)
	defer query.Free()

	// Try using valid index
	rangeNum, err := query.GetRangeNum(0)
	checkError(err)

	fmt.Printf("Number of ranges across dimension 0 is: %d\n", *rangeNum)

	// Try using valid dim name
	rangeNum, err = query.GetRangeNumFromName("rows")
	checkError(err)

	fmt.Printf("Number of ranges across dimension `rows` is: %d\n", *rangeNum)

	err = query.Finalize()
	checkError(err)
}

func getRange() {
	ctx, err := tiledb.NewContext(nil)
	checkError(err)
	defer ctx.Free()

	// Prepare the array for reading
	array, err := tiledb.NewArray(ctx, rangeArrayName)
	checkError(err)
	defer array.Free()

	err = array.Open(tiledb.TILEDB_READ)
	checkError(err)
	defer array.Close()

	// Prepare the query
	query, err := tiledb.NewQuery(ctx, array)
	checkError(err)
	defer query.Free()

	// Try using valid dimension index and range index
	start, end, err := query.GetRange(0, 0)
	checkError(err)

	fmt.Printf("Range start for dimension 0, range 0 is: %d\n", start.(int32))
	fmt.Printf("Range end for dimension 0, range 0 is: %d\n", end.(int32))

	err = query.Finalize()
	checkError(err)
}

// RunRange shows an example of creation, writing of a dense array
// and usage of range functions
func RunRange() {
	createRangeArray()
	writeRangeArray()
	addRange()
	getRangeNum()
	getRange()

	// Cleanup example so unit tests are clean
	if _, err := os.Stat(rangeArrayName); err == nil {
		err = os.RemoveAll(rangeArrayName)
		checkError(err)
	}
}
