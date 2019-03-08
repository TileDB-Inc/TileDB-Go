/**
 * @file   reading_sparse_layouts_test.go
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018 TileDB, Inc.
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
 * This is a part of the TileDB tutorial:
 *   https://docs.tiledb.io/en/latest/tutorials/reading.html
 *
 * When run, this program will create a simple 2D sparse array, write some data
 * to it, and read a slice of the data back in the layout of the user's choice
 * (passed as an argument to the program: "row", "col", or "global").
 *
 */

package examples

import (
	"fmt"
	"github.com/TileDB-Inc/TileDB-Go"
	"os"
)

// Name of array.
var readingSparseLayoutsArrayName = "reading_sparse_layouts_array"

func createReadingSparseLayoutsArray() {
	// Create a TileDB context.
	ctx, err := tiledb.NewContext(nil)
	checkError(err)

	// The array will be 4x4 with dimensions "rows" and "cols",
	// with domain [1,4].
	domain, err := tiledb.NewDomain(ctx)
	checkError(err)
	rowDim, err := tiledb.NewDimension(ctx, "rows", []int32{1, 4}, int32(2))
	checkError(err)
	colDim, err := tiledb.NewDimension(ctx, "cols", []int32{1, 4}, int32(2))
	checkError(err)
	err = domain.AddDimensions(rowDim, colDim)
	checkError(err)

	// The array will be sparse.
	schema, err := tiledb.NewArraySchema(ctx, tiledb.TILEDB_SPARSE)
	checkError(err)
	err = schema.SetDomain(domain)
	checkError(err)
	err = schema.SetCellOrder(tiledb.TILEDB_ROW_MAJOR)
	checkError(err)
	err = schema.SetTileOrder(tiledb.TILEDB_ROW_MAJOR)
	checkError(err)

	// Add a single attribute "a" so each (i,j) cell can store an integer.
	a, err := tiledb.NewAttribute(ctx, "a", tiledb.TILEDB_UINT32)
	checkError(err)
	err = schema.AddAttributes(a)
	checkError(err)

	// Create the (empty) array on disk.
	array, err := tiledb.NewArray(ctx, readingSparseLayoutsArrayName)
	checkError(err)
	err = array.Create(schema)
	checkError(err)
}

func writeReadingSparseLayoutsArray() {
	ctx, err := tiledb.NewContext(nil)
	checkError(err)

	// Prepare data for writing.
	coords := []int32{1, 1, 1, 2, 2, 2, 1, 4, 2, 3, 2, 4}
	data := []uint32{1, 2, 3, 4, 5, 6}

	// Open the array for writing and create the query.
	array, err := tiledb.NewArray(ctx, readingSparseLayoutsArrayName)
	checkError(err)
	err = array.Open(tiledb.TILEDB_WRITE)
	checkError(err)
	query, err := tiledb.NewQuery(ctx, array)
	checkError(err)
	err = query.SetLayout(tiledb.TILEDB_GLOBAL_ORDER)
	checkError(err)
	_, err = query.SetBuffer("a", data)
	checkError(err)
	_, err = query.SetCoordinates(coords)
	checkError(err)

	// Perform the write, finalize and close the array.
	err = query.Submit()
	checkError(err)
	err = query.Finalize()
	checkError(err)
	err = array.Close()
	checkError(err)
}

func readeReadingSparseLayoutsArray() {
	ctx, err := tiledb.NewContext(nil)
	checkError(err)

	// Prepare the array for reading
	array, err := tiledb.NewArray(ctx, readingSparseLayoutsArrayName)
	checkError(err)
	err = array.Open(tiledb.TILEDB_READ)
	checkError(err)

	// Non-empty domain: [1,4], [1,4]
	x, isEmpty, err := array.NonEmptyDomain()
	if !isEmpty {
		rows := x[0].Bounds.([]int32)
		cols := x[1].Bounds.([]int32)
		fmt.Printf("Non-empty domain: [%d,%d], [%d,%d]\n",
			rows[0], rows[1], cols[0], cols[1])
	}

	// Slice only rows 1, 2 and cols 2, 3, 4
	subArray := []int32{1, 2, 2, 4}

	// Prepare the vector that will hold the result
	maxElements, err := array.MaxBufferElements(subArray)
	checkError(err)
	data := make([]uint32, maxElements["a"][1])
	coords := make([]int32, maxElements[tiledb.TILEDB_COORDS][1])

	// Prepare the query
	query, err := tiledb.NewQuery(ctx, array)
	checkError(err)
	err = query.SetSubArray(subArray)
	checkError(err)
	err = query.SetLayout(tiledb.TILEDB_ROW_MAJOR)
	checkError(err)
	_, err = query.SetBuffer("a", data)
	checkError(err)
	_, err = query.SetCoordinates(coords)
	checkError(err)

	// Submit the query and close the array.
	err = query.Submit()
	checkError(err)

	// Print out the results.
	elements, err := query.ResultBufferElements()
	checkError(err)
	resultNum := elements["a"][1]
	for r := 0; r < int(resultNum); r++ {
		i := coords[2*r]
		j := coords[2*r+1]
		a := data[r]
		fmt.Printf("Cell (%d, %d) has data %d\n", i, j, a)
	}

	err = array.Close()
	checkError(err)
}

func ExampleReadingSparseLayouts() {
	createReadingSparseLayoutsArray()
	writeReadingSparseLayoutsArray()
	readeReadingSparseLayoutsArray()

	// Cleanup example so unit tests are clean
	if _, err := os.Stat(readingSparseLayoutsArrayName); err == nil {
		err = os.RemoveAll(readingSparseLayoutsArrayName)
		checkError(err)
	}

	// Output: Non-empty domain: [1,2], [1,4]
	// Cell (1, 2) has data 2
	// Cell (1, 4) has data 4
	// Cell (2, 2) has data 3
	// Cell (2, 3) has data 5
	// Cell (2, 4) has data 6
}
