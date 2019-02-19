/*
Copyright (c) 2018 TileDB, Inc.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.


This is a part of the TileDB quickstart tutorial:
https://docs.tiledb.io/en/latest/quickstart.html

When run, this program will create a simple 2D dense array, write some data
to it, and read a slice of the data back, then clean up.
For simplicity this program does not handle errors
*/
package tiledb

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

// Name of array.
var denseArrayName = "quickstart_dense"

func createDenseArray() {
	// Create a TileDB context.
	ctx, _ := NewContext(nil)

	// The array will be 4x4 with dimensions "rows" and "cols", with domain [1,4].
	domain, _ := NewDomain(ctx)
	rowDim, _ := NewDimension(ctx, "rows", []int32{1, 4}, int32(4))
	colDim, _ := NewDimension(ctx, "cols", []int32{1, 4}, int32(4))
	domain.AddDimensions(rowDim, colDim)

	// The array will be dense.
	schema, _ := NewArraySchema(ctx, TILEDB_DENSE)
	schema.SetDomain(domain)
	schema.SetCellOrder(TILEDB_ROW_MAJOR)
	schema.SetTileOrder(TILEDB_ROW_MAJOR)

	// Add a single attribute "a" so each (i,j) cell can store an integer.
	a, _ := NewAttribute(ctx, "a", TILEDB_INT32)
	schema.AddAttributes(a)

	// Create the (empty) array on disk.
	array, _ := NewArray(ctx, denseArrayName)
	array.Create(schema)
}

func writeDenseArray() {
	ctx, _ := NewContext(nil)

	// Prepare some data for the array
	data := []int32{
		1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}

	// Open the array for writing and create the query.
	array, _ := NewArray(ctx, denseArrayName)
	array.Open(TILEDB_WRITE)
	query, _ := NewQuery(ctx, array)
	query.SetLayout(TILEDB_ROW_MAJOR)
	query.SetBuffer("a", data)

	// Perform the write and close the array.
	query.Submit()
	array.Close()
}

func readDenseArray() []int32 {
	ctx, _ := NewContext(nil)

	// Prepare the array for reading
	array, _ := NewArray(ctx, denseArrayName)
	array.Open(TILEDB_READ)

	// Slice only rows 1, 2 and cols 2, 3, 4
	subArray := []int32{1, 2, 2, 4}

	// Prepare the vector that will hold the result (of size 6 elements)
	data := make([]int32, 6)

	// Prepare the query
	query, _ := NewQuery(ctx, array)
	query.SetSubArray(subArray)
	query.SetLayout(TILEDB_ROW_MAJOR)
	query.SetBuffer("a", data)

	// Submit the query and close the array.
	query.Submit()
	array.Close()

	// Print out the results.
	fmt.Println(data)

	return data
}

// ExampleDenseArray shows and example creation, writing and reading of a dense
// array
func TestDenseArray(t *testing.T) {
	createDenseArray()
	writeDenseArray()

	data := readDenseArray()
	expectedData := []int32{2, 3, 4, 6, 7, 8}
	assert.EqualValues(t, data, expectedData)

	// Cleanup example so unit tests are clean
	if _, err := os.Stat(denseArrayName); err == nil {
		os.RemoveAll(denseArrayName)
	}
}
