/**
 * @file   rerading_range_test.go
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2019 TileDB, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 *
 * @section DESCRIPTION
 *
 * This is a part of the TileDB reading_range tutorial:
 *   https://docs.tiledb.io/en/latest/reading_range.html
 *
 * When run, this program will create a simple 2D dense array, write some data
 * to it, and read a slice of the data back in the layout of the user's choice
 * (passed as an argument to the program: "row", "col", or "global").
 *
 */

package examples

import (
	"fmt"
	"os"

	tiledb "github.com/TileDB-Inc/TileDB-Go"
)

// Name of array.
var readRangeArrayName = "read_range_array"

func createReadRangeArray() {
	// Create a TileDB context.
	ctx, err := tiledb.NewContext(nil)
	checkError(err)

	// The array will be 4x4 with dimensions "rows" and "cols", with domain [1,4].
	domain, err := tiledb.NewDomain(ctx)
	checkError(err)
	rowDim, err := tiledb.NewDimension(ctx, "rows", []int32{1, 4}, int32(4))
	checkError(err)
	colDim, err := tiledb.NewDimension(ctx, "cols", []int32{1, 4}, int32(4))
	checkError(err)
	err = domain.AddDimensions(rowDim, colDim)
	checkError(err)

	// The array will be dense.
	schema, err := tiledb.NewArraySchema(ctx, tiledb.TILEDB_DENSE)
	err = schema.SetDomain(domain)
	checkError(err)
	err = schema.SetCellOrder(tiledb.TILEDB_ROW_MAJOR)
	checkError(err)
	err = schema.SetTileOrder(tiledb.TILEDB_ROW_MAJOR)
	checkError(err)

	// Add a single attribute "a" so each (i,j) cell can store an integer.
	a, err := tiledb.NewAttribute(ctx, "a", tiledb.TILEDB_INT32)
	checkError(err)
	err = schema.AddAttributes(a)
	checkError(err)

	// Create the (empty) array on disk.
	array, err := tiledb.NewArray(ctx, readRangeArrayName)
	checkError(err)
	err = array.Create(schema)
	checkError(err)
}

func writeRearRangeArray() {
	ctx, err := tiledb.NewContext(nil)
	checkError(err)

	// Prepare some data for the array
	data := []int32{
		1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}

	// Open the array for writing and create the query.
	array, err := tiledb.NewArray(ctx, readRangeArrayName)
	checkError(err)
	err = array.Open(tiledb.TILEDB_WRITE)
	checkError(err)
	query, err := tiledb.NewQuery(ctx, array)
	checkError(err)
	err = query.SetLayout(tiledb.TILEDB_ROW_MAJOR)
	checkError(err)
	_, err = query.SetBuffer("a", data)
	checkError(err)

	// Perform the write and close the array.
	err = query.Submit()
	checkError(err)
	err = array.Close()
	checkError(err)
}

func readReadRangeArray(dimIdx uint32) {
	ctx, err := tiledb.NewContext(nil)
	checkError(err)

	// Prepare the array for reading
	array, err := tiledb.NewArray(ctx, readRangeArrayName)
	checkError(err)
	err = array.Open(tiledb.TILEDB_READ)
	checkError(err)

	// Prepare the vector that will hold the result (of size 6 elements)
	data := make([]int32, 12)

	// Prepare the query
	query, err := tiledb.NewQuery(ctx, array)
	checkError(err)

	err = query.AddRange(dimIdx, int32(1), int32(1))
	checkError(err)
	err = query.AddRange(dimIdx, int32(3), int32(4))
	checkError(err)

	numOfRanges, err := query.GetRangeNum(dimIdx)
	checkError(err)
	fmt.Printf("Num of Ranges: %d\n", *numOfRanges)

	var I uint64
	for I = 0; I < *numOfRanges; I++ {
		start, end, err := query.GetRange(dimIdx, I)
		checkError(err)
		fmt.Printf("Range for dimension: %d, start: %v, end: %v\n", dimIdx, start, end)
	}

	ranges, err := query.GetRanges()
	checkError(err)

	fmt.Printf("Ranges: %v\n", ranges)

	_, err = query.SetBuffer("a", data)
	checkError(err)

	// Submit the query and close the array.
	err = query.Submit()
	checkError(err)
	err = array.Close()
	checkError(err)

	// Print out the results.
	fmt.Println(data)
}

// ExampleReadRangeArray shows and example creation, writing and range reading
// of a dense array
func ExampleReadRangeArray() {
	createReadRangeArray()
	writeRearRangeArray()
	// Rows
	readReadRangeArray(0)
	// Columns
	readReadRangeArray(1)

	// Cleanup example so unit tests are clean
	if _, err := os.Stat(readRangeArrayName); err == nil {
		err = os.RemoveAll(readRangeArrayName)
		checkError(err)
	}

	// Output: Num of Ranges: 2
	// Range for dimension: 0, start: 1, end: 1
	// Range for dimension: 0, start: 3, end: 4
	// Ranges: [[{1 1} {3 4}] [{1 4}]]
	// [1 2 3 4 9 10 11 12 13 14 15 16]
	// Num of Ranges: 2
	// Range for dimension: 1, start: 1, end: 1
	// Range for dimension: 1, start: 3, end: 4
	// Ranges: [[{1 4}] [{1 1} {3 4}]]
	// [1 3 4 5 7 8 9 11 12 13 15 16]
}

//  1  2  3  4
//  5  6  7  8
//  9 10 11 12
// 13 14 15 16
