/**
 * @file   quickstart_sparse_test.go
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
 * This is a part of the TileDB quickstart tutorial:
 * 	 https://docs.tiledb.io/en/latest/quickstart.html
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
	"unsafe"

	tiledb "github.com/TileDB-Inc/TileDB-Go"
)

// Name of array.
var sparseArrayName = "quickstart_sparse"

func createSparseArray() {
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
	a, err := tiledb.NewAttribute(ctx, "a", tiledb.TILEDB_UINT32)
	checkError(err)
	err = schema.AddAttributes(a)
	checkError(err)

	// Create the (empty) array on disk.
	array, err := tiledb.NewArray(ctx, sparseArrayName)
	checkError(err)
	err = array.Create(schema)
	checkError(err)
}

func writeSparseArray() {
	ctx, err := tiledb.NewContext(nil)
	checkError(err)

	// Write some simple data to cells (1, 1), (2, 4) and (2, 3).
	buffD1 := []int32{1, 2, 2}
	buffD2 := []int32{1, 4, 3}
	data := []uint32{1, 2, 3}

	// Open the array for writing and create the query.
	array, err := tiledb.NewArray(ctx, sparseArrayName)
	checkError(err)
	err = array.Open(tiledb.TILEDB_WRITE)
	checkError(err)
	query, err := tiledb.NewQuery(ctx, array)
	checkError(err)
	err = query.SetLayout(tiledb.TILEDB_UNORDERED)
	checkError(err)
	_, err = query.SetBuffer("rows", buffD1)
	checkError(err)
	_, err = query.SetBuffer("cols", buffD2)
	checkError(err)
	_, err = query.SetBuffer("a", data)
	checkError(err)

	// Perform the write and close the array.
	err = query.Submit()
	checkError(err)
	err = array.Close()
	checkError(err)
}

func readSparseArray() {
	ctx, err := tiledb.NewContext(nil)
	checkError(err)

	// Prepare the array for reading
	array, err := tiledb.NewArray(ctx, sparseArrayName)
	checkError(err)
	err = array.Open(tiledb.TILEDB_READ)
	checkError(err)

	// Prepare the query
	query, err := tiledb.NewQuery(ctx, array)
	checkError(err)

	err = query.SetLayout(tiledb.TILEDB_UNORDERED)
	checkError(err)
	err = query.AddRange(0, int32(1), int32(2))
	checkError(err)
	err = query.AddRange(1, int32(1), int32(4))
	checkError(err)

	size, err := query.EstResultSize("a")
	checkError(err)
	fmt.Printf("Estimated query size in bytes for attribute 'a': %d\n", *size)
	buffAR := make([]uint32, (*size)/uint64(unsafe.Sizeof(int32(0))))

	size, err = query.EstResultSize("rows")
	checkError(err)
	fmt.Printf("Estimated query size in bytes for dimension 'rows': %d\n", *size)
	buffD1R := make([]int32, (*size)/uint64(unsafe.Sizeof(int32(0))))

	size, err = query.EstResultSize("cols")
	checkError(err)
	fmt.Printf("Estimated query size in bytes for dimension 'cols': %d\n", *size)
	buffD2R := make([]int32, (*size)/uint64(unsafe.Sizeof(int32(0))))

	_, err = query.SetBuffer("rows", buffD1R)
	checkError(err)
	_, err = query.SetBuffer("cols", buffD2R)
	checkError(err)
	_, err = query.SetBuffer("a", buffAR)
	checkError(err)

	// Submit the query and close the array.
	err = query.Submit()
	checkError(err)

	for i, aVal := range buffAR {
		fmt.Printf("Cell (%d, %d) has data %d\n", buffD1R[i], buffD2R[i], aVal)
	}

	err = array.Close()
	checkError(err)
}

// ExampleSparseArray shows and example creation, writing and reading of a
// sparse array
func ExampleSparseArray() {
	createSparseArray()
	writeSparseArray()
	readSparseArray()

	// Cleanup example so unit tests are clean
	if _, err := os.Stat(sparseArrayName); err == nil {
		err = os.RemoveAll(sparseArrayName)
		checkError(err)
	}

	// Output: Estimated query size in bytes for attribute 'a': 12
	// Estimated query size in bytes for dimension 'rows': 12
	// Estimated query size in bytes for dimension 'cols': 12
	// Cell (1, 1) has data 1
	// Cell (2, 3) has data 3
	// Cell (2, 4) has data 2
}
