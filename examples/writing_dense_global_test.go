/**
 * @file   writing_dense_global_test.go
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
 *   https://docs.tiledb.io/en/latest/tutorials/writing-dense.html
 *
 * When run, this program will create a simple 2D dense array, write some data
 * to it in global layout, and read the entire array data back.
 */

package examples

import (
	"fmt"
	"github.com/TileDB-Inc/TileDB-Go"
	"os"
)

// Name of array.
var denseGlobalArrayName = "writing_dense_global_array"

func createDenseGlobalArray() {
	// Create a TileDB context.
	ctx, err := tiledb.NewContext(nil)
	checkError(err)

	// The array will be 4x4 with dimensions "rows" and "cols", with domain [1,4]
	// and space tiles 2x2
	domain, err := tiledb.NewDomain(ctx)
	checkError(err)
	rowDim, err := tiledb.NewDimension(ctx, "rows", []int32{1, 4}, int32(2))
	checkError(err)
	colDim, err := tiledb.NewDimension(ctx, "cols", []int32{1, 4}, int32(2))
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
	array, err := tiledb.NewArray(ctx, denseGlobalArrayName)
	checkError(err)
	err = array.Create(schema)
	checkError(err)
}

func writeDenseGlobalArray() {
	ctx, err := tiledb.NewContext(nil)
	checkError(err)

	subarray := []int32{1, 4, 1, 2}

	// Open the array for writing.
	array, err := tiledb.NewArray(ctx, denseGlobalArrayName)
	checkError(err)
	err = array.Open(tiledb.TILEDB_WRITE)
	checkError(err)
	query, err := tiledb.NewQuery(ctx, array)
	checkError(err)

	// First submission
	data := []int32{1, 2, 3, 4}
	err = query.SetLayout(tiledb.TILEDB_GLOBAL_ORDER)
	checkError(err)
	_, err = query.SetBuffer("a", data)
	checkError(err)
	err = query.SetSubArray(subarray)
	checkError(err)

	// Perform the write
	err = query.Submit()
	checkError(err)

	// Second submission, after updating the buffer contents
	for i := 0; i < 4; i++ {
		data[i] = int32(5 + i)
	}
	err = query.Submit()
	checkError(err)

	// IMPORTANT!
	err = query.Finalize()
	checkError(err)
	err = array.Close()
	checkError(err)
}

func readDenseGlobalArray() {
	ctx, err := tiledb.NewContext(nil)
	checkError(err)

	// Prepare the array for reading
	array, err := tiledb.NewArray(ctx, denseGlobalArrayName)
	checkError(err)
	err = array.Open(tiledb.TILEDB_READ)
	checkError(err)

	// Read the entire array
	subArray := []int32{1, 4, 1, 4}

	// Prepare the vector that will hold the result (of size 16 elements)
	data := make([]int32, 16)

	// Prepare the query
	query, err := tiledb.NewQuery(ctx, array)
	checkError(err)
	err = query.SetSubArray(subArray)
	checkError(err)
	err = query.SetLayout(tiledb.TILEDB_ROW_MAJOR)
	checkError(err)
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

func ExampleWritingDenseGlobal() {
	createDenseGlobalArray()
	writeDenseGlobalArray()
	readDenseGlobalArray()

	// Cleanup example so unit tests are clean
	if _, err := os.Stat(denseGlobalArrayName); err == nil {
		err = os.RemoveAll(denseGlobalArrayName)
		checkError(err)
	}

	// Output: [1 2 -2147483648 -2147483648 3 4 -2147483648 -2147483648 5 6 -2147483648 -2147483648 7 8 -2147483648 -2147483648]
}
