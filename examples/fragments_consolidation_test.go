/**
 * @file   fragments_consolidation_test.go
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
 *   https://docs.tiledb.io/en/latest/tutorials/fragments-consolidation.html
 *
 * When run, this program will create a simple 2D dense array, write some data
 * with three queries (creating three fragments), optionally consolidate
 * and read the entire array data back.
 */

package examples

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
	checkError(err)
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
	array, err := tiledb.NewArray(ctx, fragmentsConsolidationArrayName)
	checkError(err)
	err = array.Create(schema)
	checkError(err)
}

func writeFragmentsConsolidationArray1() {
	ctx, err := tiledb.NewContext(nil)
	checkError(err)

	// Prepare some data for the array
	data := []int32{1, 2, 3, 4, 5, 6, 7, 8}
	subarray := []int32{1, 2, 1, 4}

	// Create the query
	array, err := tiledb.NewArray(ctx, fragmentsConsolidationArrayName)
	checkError(err)
	err = array.Open(tiledb.TILEDB_WRITE)
	checkError(err)
	query, err := tiledb.NewQuery(ctx, array)
	checkError(err)
	err = query.SetLayout(tiledb.TILEDB_ROW_MAJOR)
	checkError(err)
	_, err = query.SetBuffer("a", data)
	checkError(err)
	err = query.SetSubArray(subarray)
	checkError(err)

	// Perform the write and close the array.
	err = query.Submit()
	checkError(err)
	err = array.Close()
	checkError(err)
}

func writeFragmentsConsolidationArray2() {
	ctx, err := tiledb.NewContext(nil)
	checkError(err)

	// Prepare some data for the array
	data := []int32{101, 102, 103, 104}
	subarray := []int32{2, 3, 2, 3}

	// Create the query
	array, err := tiledb.NewArray(ctx, fragmentsConsolidationArrayName)
	checkError(err)
	err = array.Open(tiledb.TILEDB_WRITE)
	checkError(err)
	query, err := tiledb.NewQuery(ctx, array)
	checkError(err)
	err = query.SetLayout(tiledb.TILEDB_ROW_MAJOR)
	checkError(err)
	_, err = query.SetBuffer("a", data)
	checkError(err)
	err = query.SetSubArray(subarray)
	checkError(err)

	// Perform the write and close the array.
	err = query.Submit()
	checkError(err)
	err = array.Close()
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
	err = array.Open(tiledb.TILEDB_WRITE)
	checkError(err)
	query, err := tiledb.NewQuery(ctx, array)
	checkError(err)
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

	err = array.Close()
	checkError(err)
}

func readFragmentsConsolidationArray() {
	// Create TileDB context
	ctx, err := tiledb.NewContext(nil)
	checkError(err)

	// Prepare the array for reading
	array, err := tiledb.NewArray(ctx, fragmentsConsolidationArrayName)
	checkError(err)
	err = array.Open(tiledb.TILEDB_READ)
	checkError(err)

	// Read the entire array
	subArray := []int32{1, 4, 1, 4}

	// Prepare the query
	query, err := tiledb.NewQuery(ctx, array)
	checkError(err)
	err = query.SetSubArray(subArray)
	checkError(err)

	// Prepare the vector that will hold the result
	maxElMap, err := array.MaxBufferElements(subArray)
	checkError(err)

	data := make([]int32, maxElMap["a"][1])
	rows := make([]int32, maxElMap["rows"][1])
	cols := make([]int32, maxElMap["cols"][1])

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

	err = array.Close()
	checkError(err)
}

func ExampleFragmentsConsolidationArray() {
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

	// Output: Num of fragments: 1
	// Cell (1, 1) has data 201
	// Cell (1, 2) has data 2
	// Cell (1, 3) has data 3
	// Cell (1, 4) has data 4
	// Cell (2, 1) has data 5
	// Cell (2, 2) has data 101
	// Cell (2, 3) has data 102
	// Cell (2, 4) has data 8
	// Cell (3, 1) has data -2147483648
	// Cell (3, 2) has data 103
	// Cell (3, 3) has data 104
	// Cell (3, 4) has data 202
	// Cell (4, 1) has data -2147483648
	// Cell (4, 2) has data -2147483648
	// Cell (4, 3) has data -2147483648
	// Cell (4, 4) has data -2147483648
}
