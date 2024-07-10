package examples_lib

import (
	"encoding/json"
	"fmt"

	tiledb "github.com/TileDB-Inc/TileDB-Go"
)

func createReadRangeArray(dir string) {
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
	array, err := tiledb.NewArray(ctx, dir)
	checkError(err)
	defer array.Free()

	err = array.Create(schema)
	checkError(err)
}

func writeRearRangeArray(dir string) {
	ctx, err := tiledb.NewContext(nil)
	checkError(err)
	defer ctx.Free()

	// Prepare some data for the array
	data := []int32{
		1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}

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

	err = query.SetLayout(tiledb.TILEDB_ROW_MAJOR)
	checkError(err)
	_, err = query.SetDataBuffer("a", data)
	checkError(err)

	// Perform the write and close the array.
	err = query.Submit()
	checkError(err)

	err = query.Finalize()
	checkError(err)
}

func readReadRangeArray(dir string, dimIdx uint32) {
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

	// Prepare the vector that will hold the result (of size 6 elements)
	data := make([]int32, 12)

	// Prepare the query
	query, err := tiledb.NewQuery(ctx, array)
	checkError(err)
	defer query.Free()

	// Prepare the subarray
	subarray, err := array.NewSubarray()
	checkError(err)
	defer subarray.Free()

	err = subarray.AddRange(dimIdx, tiledb.MakeRange[int32](1, 1))
	checkError(err)
	err = subarray.AddRange(dimIdx, tiledb.MakeRange[int32](3, 4))
	checkError(err)
	err = query.SetSubarray(subarray)
	checkError(err)

	numOfRanges, err := subarray.GetRangeNum(dimIdx)
	checkError(err)
	fmt.Printf("Num of Ranges: %d\n", numOfRanges)

	var I uint64
	for I = 0; I < numOfRanges; I++ {
		r, err := subarray.GetRange(dimIdx, I)
		checkError(err)
		start, end := r.Endpoints()
		fmt.Printf("Range for dimension: %d, start: %v, end: %v\n", dimIdx, start, end)
	}

	// subarray.GetRanges does not marshal to valid JSON.
	rangeMap, err := query.GetRanges()
	checkError(err)

	fmt.Printf("Ranges: %v\n", rangeMap)

	rangesJSON, err := json.Marshal(rangeMap)
	checkError(err)

	// Print ranges json
	fmt.Printf("Ranges JSON: %s\n", string(rangesJSON))

	_, err = query.SetDataBuffer("a", data)
	checkError(err)

	// Submit the query and close the array.
	err = query.Submit()
	checkError(err)

	err = query.Finalize()
	checkError(err)

	// Print out the results.
	// fmt.Println(data)
}

// RunReadRangeArray shows and example creation, writing and range reading
// of a dense array
func RunReadRangeArray() {
	tmpDir := temp("read_range_array")
	defer cleanup(tmpDir)

	createReadRangeArray(tmpDir)
	writeRearRangeArray(tmpDir)
	// Rows
	readReadRangeArray(tmpDir, 0)
	// Columns
	readReadRangeArray(tmpDir, 1)
}

//  1  2  3  4
//  5  6  7  8
//  9 10 11 12
// 13 14 15 16

// {
// 	"cols":[
// 	   {
// 		  "start":1,
// 		  "end":4
// 	   }
// 	],
// 	"rows":[
// 	   {
// 		  "start":1,
// 		  "end":1
// 	   },
// 	   {
// 		  "start":3,
// 		  "end":4
// 	   }
// 	]
//  }
