/**
 * @file   writing_sparse_global_test.go
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
 *   https://docs.tiledb.io/en/latest/tutorials/writing-sparse.html
 *
 * When run, this program will create a simple 2D sparse array, write some data
 * to it in global order, and read the data back.
 *
 */

package examples

import (
	"fmt"
	"os"

	tiledb "github.com/TileDB-Inc/TileDB-Go"
)

// Name of array.
var sparseGlobalArrayName = "writing_sparse_global_array"

func createSparseGlobalArray() {
	// Create a TileDB context.
	ctx, err := tiledb.NewContext(nil)
	checkError(err)

	// The array will be 4x4 with dimensions "rows" and "cols",
	// with domain [1,4].
	domain, err := tiledb.NewDomain(ctx)
	checkError(err)
	rowDim, err := tiledb.NewDimension(ctx, "rows", []int32{1, 4}, int32(4))
	checkError(err)
	colDim, err := tiledb.NewDimension(ctx, "cols", []int32{1, 4}, int32(4))
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
	a, err := tiledb.NewAttribute(ctx, "a", tiledb.TILEDB_INT32)
	checkError(err)
	err = schema.AddAttributes(a)
	checkError(err)

	// Create the (empty) array on disk.
	array, err := tiledb.NewArray(ctx, sparseGlobalArrayName)
	checkError(err)
	err = array.Create(schema)
	checkError(err)
}

func execQueryGlobalOrder(ctx *tiledb.Context, array *tiledb.Array,
	data []int32, buffD1 []int32, buffD2 []int32) {
	query, err := tiledb.NewQuery(ctx, array)
	checkError(err)
	err = query.SetLayout(tiledb.TILEDB_GLOBAL_ORDER)
	checkError(err)

	// Submit query
	_, err = query.SetBuffer("a", data)
	checkError(err)
	_, err = query.SetBuffer("rows", buffD1)
	checkError(err)
	_, err = query.SetBuffer("cols", buffD2)
	checkError(err)
	// Perform the write.
	err = query.Submit()
	checkError(err)
	// IMPORTANT!
	err = query.Finalize()
	checkError(err)
}

func writeSparseGlobalArray() {
	ctx, err := tiledb.NewContext(nil)
	checkError(err)

	// Open the array for writing.
	array, err := tiledb.NewArray(ctx, sparseGlobalArrayName)
	checkError(err)
	err = array.Open(tiledb.TILEDB_WRITE)
	checkError(err)

	// Query 1
	buffD1 := []int32{1, 2}
	buffD2 := []int32{1, 4}
	data1 := []int32{1, 2}
	execQueryGlobalOrder(ctx, array, data1, buffD1, buffD2)

	// Query 2
	buffD1 = []int32{2}
	buffD2 = []int32{3}
	data2 := []int32{3}
	execQueryGlobalOrder(ctx, array, data2, buffD1, buffD2)

	err = array.Close()
	checkError(err)
}

func readSparseGlobalArray() {
	ctx, err := tiledb.NewContext(nil)
	checkError(err)

	// Prepare the array for reading
	array, err := tiledb.NewArray(ctx, sparseGlobalArrayName)
	checkError(err)
	err = array.Open(tiledb.TILEDB_READ)
	checkError(err)

	// Read the whole array
	subArray := []int32{1, 4, 1, 4}

	// Prepare the vector that will hold the results
	// We take the upper bound on the result size as we do not know how large
	// a buffer is needed since the array is sparse
	maxElements, err := array.MaxBufferElements(subArray)
	checkError(err)
	data := make([]int32, maxElements["a"][1])
	rows := make([]int32, maxElements["rows"][1])
	cols := make([]int32, maxElements["cols"][1])

	// Prepare the query
	query, err := tiledb.NewQuery(ctx, array)
	checkError(err)
	err = query.SetSubArray(subArray)
	checkError(err)
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

func ExampleWritingSparseGlobal() {
	createSparseGlobalArray()
	writeSparseGlobalArray()
	readSparseGlobalArray()

	// Cleanup example so unit tests are clean
	if _, err := os.Stat(sparseGlobalArrayName); err == nil {
		err = os.RemoveAll(sparseGlobalArrayName)
		checkError(err)
	}

	// Output: Cell (1, 1) has data 1
	// Cell (2, 3) has data 3
	// Cell (2, 4) has data 2
}
