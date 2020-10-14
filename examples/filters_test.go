/**
 * @file   filters.go
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
 * This is a part of the TileDB filters tutorial:
 *   https://docs.tiledb.io/en/latest/tutorials/filters.html
 *
 * When run, this program will create a 2D sparse array with several filters,
 * write some data to it, and read a slice of the data back.
 *
 */

package examples

import (
	"fmt"
	"os"

	tiledb "github.com/TileDB-Inc/TileDB-Go"
)

// Name of array.
var filtersArrayName = "filters_array"

func createFilterArray() {
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

	// Create two fixed-length attributes "a1" and "a2"
	a1, err := tiledb.NewAttribute(ctx, "a1", tiledb.TILEDB_UINT32)
	checkError(err)
	a2, err := tiledb.NewAttribute(ctx, "a2", tiledb.TILEDB_INT32)
	checkError(err)

	// a1 will be filtered by bit width reduction followed by zstd
	// compression.
	bitWidthReduction, err := tiledb.NewFilter(ctx,
		tiledb.TILEDB_FILTER_BIT_WIDTH_REDUCTION)
	checkError(err)
	compressionZstd, err := tiledb.NewFilter(ctx, tiledb.TILEDB_FILTER_ZSTD)
	checkError(err)
	a1Filters, err := tiledb.NewFilterList(ctx)
	checkError(err)
	err = a1Filters.AddFilter(bitWidthReduction)
	checkError(err)
	err = a1Filters.AddFilter(compressionZstd)
	checkError(err)
	err = a1.SetFilterList(a1Filters)
	checkError(err)

	// a2 will just have a single gzip compression filter.
	compressionGzip, err := tiledb.NewFilter(ctx, tiledb.TILEDB_FILTER_GZIP)
	checkError(err)
	a2Filters, err := tiledb.NewFilterList(ctx)
	checkError(err)
	err = a2Filters.AddFilter(compressionGzip)
	checkError(err)
	err = a2.SetFilterList(a2Filters)

	// Add the attributes
	err = schema.AddAttributes(a1)
	checkError(err)
	err = schema.AddAttributes(a2)
	checkError(err)

	// Create the (empty) array on disk.
	array, err := tiledb.NewArray(ctx, filtersArrayName)
	checkError(err)
	err = array.Create(schema)
	checkError(err)
}

func writeFiltersArray() {
	ctx, err := tiledb.NewContext(nil)
	checkError(err)

	// Write some simple data to cells (1, 1), (2, 4) and (2, 3).
	buffD1 := []int32{1, 2, 2}
	buffD2 := []int32{1, 4, 3}
	dataA1 := []uint32{1, 2, 3}
	dataA2 := []int32{-1, -2, -3}

	// Open the array for writing and create the query.
	array, err := tiledb.NewArray(ctx, filtersArrayName)
	checkError(err)
	err = array.Open(tiledb.TILEDB_WRITE)
	checkError(err)
	query, err := tiledb.NewQuery(ctx, array)
	checkError(err)
	err = query.SetLayout(tiledb.TILEDB_UNORDERED)
	checkError(err)
	_, err = query.SetBuffer("a1", dataA1)
	checkError(err)
	_, err = query.SetBuffer("a2", dataA2)
	checkError(err)
	_, err = query.SetBuffer("rows", buffD1)
	checkError(err)
	_, err = query.SetBuffer("cols", buffD2)
	checkError(err)

	// Perform the write and close the array.
	err = query.Submit()
	checkError(err)
	err = array.Close()
	checkError(err)
}

func readFiltersArray() {
	ctx, err := tiledb.NewContext(nil)
	checkError(err)

	// Prepare the array for reading
	array, err := tiledb.NewArray(ctx, filtersArrayName)
	checkError(err)
	err = array.Open(tiledb.TILEDB_READ)
	checkError(err)

	// Slice only rows 1, 2 and cols 2, 3, 4
	subArray := []int32{1, 2, 2, 4}

	// Prepare the vector that will hold the results
	// We take the upper bound on the result size as we do not know how large
	// a buffer is needed since the array is sparse
	maxElements, err := array.MaxBufferElements(subArray)
	checkError(err)
	data := make([]uint32, maxElements["a1"][1])
	rows := make([]int32, maxElements["rows"][1])
	cols := make([]int32, maxElements["cols"][1])

	// Prepare the query
	query, err := tiledb.NewQuery(ctx, array)
	checkError(err)
	err = query.SetSubArray(subArray)
	checkError(err)
	err = query.SetLayout(tiledb.TILEDB_ROW_MAJOR)
	checkError(err)
	_, err = query.SetBuffer("a1", data)
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
	resultNum := elements["a1"][1]
	for r := 0; r < int(resultNum); r++ {
		i := rows[r]
		j := cols[r]
		a := data[r]
		fmt.Printf("Cell (%d, %d) has data %d\n", i, j, a)
	}

	err = array.Close()
	checkError(err)
}

// ExampleSparseArray shows and example creation, writing and reading of a
// sparse array
func ExampleFiltersArray() {
	createFilterArray()
	writeFiltersArray()
	readFiltersArray()

	// Cleanup example so unit tests are clean
	if _, err := os.Stat(filtersArrayName); err == nil {
		err = os.RemoveAll(filtersArrayName)
		checkError(err)
	}

	// Output: Cell (2, 3) has data 3
	// Cell (2, 4) has data 2
}
