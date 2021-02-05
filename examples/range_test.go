/**
 * @file   range_test.go
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
 * This is a part of the TileDB range tutorial:
 *   https://docs.tiledb.io/en/latest/range.html
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
var rangeArrayName = "range_array"

func createRangeArray() {
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
	array, err := tiledb.NewArray(ctx, rangeArrayName)
	checkError(err)
	err = array.Create(schema)
	checkError(err)
}

func writeRangeArray() {
	ctx, err := tiledb.NewContext(nil)
	checkError(err)

	// Prepare some data for the array
	data := []int32{
		1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}

	// Open the array for writing and create the query.
	array, err := tiledb.NewArray(ctx, rangeArrayName)
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

func addRange() {
	ctx, err := tiledb.NewContext(nil)
	checkError(err)

	// Prepare the array for reading
	array, err := tiledb.NewArray(ctx, rangeArrayName)
	checkError(err)
	err = array.Open(tiledb.TILEDB_READ)
	checkError(err)

	// Prepare the query
	query, err := tiledb.NewQuery(ctx, array)
	checkError(err)

	// Try with invalid dimension types
	err = query.AddRange(0, float32(1), float32(3))
	fmt.Println(err)

	// Try with invalid dimension index
	err = query.AddRange(2, int32(1), int32(3))
	fmt.Println(err)

	// Try using valid index, range
	err = query.AddRange(0, int32(1), int32(3))
	checkError(err)

	err = array.Close()
	checkError(err)
}

func getRangeNum() {
	ctx, err := tiledb.NewContext(nil)
	checkError(err)

	// Prepare the array for reading
	array, err := tiledb.NewArray(ctx, rangeArrayName)
	checkError(err)
	err = array.Open(tiledb.TILEDB_READ)
	checkError(err)

	// Prepare the query
	query, err := tiledb.NewQuery(ctx, array)
	checkError(err)

	// Try using valid index
	rangeNum, err := query.GetRangeNum(0)
	checkError(err)

	fmt.Printf("Number of ranges across dimension 0 is: %d\n", *rangeNum)

	// Try using valid dim name
	rangeNum, err = query.GetRangeNumFromName("rows")
	checkError(err)

	fmt.Printf("Number of ranges across dimension `rows` is: %d\n", *rangeNum)

	err = array.Close()
	checkError(err)
}

func getRange() {
	ctx, err := tiledb.NewContext(nil)
	checkError(err)

	// Prepare the array for reading
	array, err := tiledb.NewArray(ctx, rangeArrayName)
	checkError(err)
	err = array.Open(tiledb.TILEDB_READ)
	checkError(err)

	// Prepare the query
	query, err := tiledb.NewQuery(ctx, array)
	checkError(err)

	// Try using valid dimension index and range index
	start, end, err := query.GetRange(0, 0)
	checkError(err)

	fmt.Printf("Range start for dimension 0, range 0 is: %d\n", start.(int32))
	fmt.Printf("Range end for dimension 0, range 0 is: %d\n", end.(int32))

	err = array.Close()
	checkError(err)
}

// ExampleRange shows an example of creation, writing of a dense array
// and usage of range functions
func ExampleRange() {
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

	// Output: Error adding query range: [TileDB::Dimension] Error: Range [1065353216, 1077936128] is out of domain bounds [1, 4] on dimension 'rows'
	// Error adding query range: [TileDB::Query] Error: Cannot add range; Invalid dimension index
	// Number of ranges across dimension 0 is: 1
	// Number of ranges across dimension `rows` is: 1
	// Range start for dimension 0, range 0 is: 1
	// Range end for dimension 0, range 0 is: 4
}
