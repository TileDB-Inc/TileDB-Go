/**
 * @file   using_tiledb_stats_test.go
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
 *   https://docs.tiledb.io/en/latest/performance/using-tiledb-statistics.html
 *
 * When run, this program will create a 0.5GB dense array, and enable the
 * TileDB statistics surrounding reads from the array.
 *
 */

package examples

import (
	"os"

	tiledb "github.com/TileDB-Inc/TileDB-Go"
)

// Name of array.
var statsArrayName = "stats_array"

func createStatsArray(rowTileExtent uint32, colTileExtent uint32) {
	// Create a TileDB context.
	ctx, err := tiledb.NewContext(nil)
	checkError(err)

	// Define a domain
	domain, err := tiledb.NewDomain(ctx)
	checkError(err)
	rowDim, err := tiledb.NewDimension(ctx,
		"rows", []uint32{1, 12000}, rowTileExtent)
	checkError(err)
	colDim, err := tiledb.NewDimension(ctx,
		"cols", []uint32{1, 12000}, colTileExtent)
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
	array, err := tiledb.NewArray(ctx, statsArrayName)
	checkError(err)
	err = array.Create(schema)
	checkError(err)
}

func writeStatsArray() {
	ctx, err := tiledb.NewContext(nil)
	checkError(err)

	// Prepare some data for the array
	values := make([]int32, 12000*12000)
	for i := 0; i < len(values); i++ {
		values[i] = int32(i)
	}

	// Create the query
	array, err := tiledb.NewArray(ctx, statsArrayName)
	checkError(err)
	err = array.Open(tiledb.TILEDB_WRITE)
	checkError(err)
	query, err := tiledb.NewQuery(ctx, array)
	checkError(err)
	err = query.SetLayout(tiledb.TILEDB_ROW_MAJOR)
	checkError(err)
	_, err = query.SetBuffer("a", values)
	checkError(err)

	// Perform the write and close the array.
	err = query.Submit()
	checkError(err)
	err = array.Close()
	checkError(err)
}

func readStatsArray() {
	// Create TileDB context
	ctx, err := tiledb.NewContext(nil)
	checkError(err)

	// Prepare the array for reading
	array, err := tiledb.NewArray(ctx, statsArrayName)
	checkError(err)
	err = array.Open(tiledb.TILEDB_READ)
	checkError(err)

	// Read a slice of 3,000 rows.
	subArray := []uint32{1, 3000, 1, 12000}

	// Prepare the query
	query, err := tiledb.NewQuery(ctx, array)
	checkError(err)
	err = query.SetSubArray(subArray)
	checkError(err)

	// Prepare the vector that will hold the result
	bufferElements, err := query.EstimateBufferElements()
	checkError(err)
	data := make([]int32, bufferElements["a"][1])

	err = query.SetLayout(tiledb.TILEDB_ROW_MAJOR)
	checkError(err)
	_, err = query.SetBuffer("a", data)
	checkError(err)

	// Enable the stats for the read query
	err = tiledb.StatsEnable()
	checkError(err)

	// Submit the query
	err = query.Submit()
	checkError(err)

	// Print the report
	err = tiledb.StatsDumpSTDOUT()
	checkError(err)
	err = tiledb.StatsDisable()
	checkError(err)

	// Close the array
	err = array.Close()
	checkError(err)
}

func ExampleUsingTileDBStats() {
	createStatsArray(1, 12000)
	writeStatsArray()
	readStatsArray()

	// Cleanup example so unit tests are clean
	if _, err := os.Stat(statsArrayName); err == nil {
		err = os.RemoveAll(statsArrayName)
		checkError(err)
	}
}
