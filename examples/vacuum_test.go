/**
 * @file   vacuum_test.go
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2020 TileDB, Inc.
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
 * When run, this program will create a simple 2D sparse array, write some data
 * to it, write again and read num of fragments. Then read from array,
 * consolidate and again read num of fragmens. Then vacuum and read number of
 * fragments.Finally will read from array to verify data read are the same as
 * in first read
 *
 */

package examples

import (
	"fmt"
	"os"

	tiledb "github.com/TileDB-Inc/TileDB-Go"
)

// Name of array.
var vaccuumSparseArrayName = "vacuum_sparse"

func createVacuumSparseArray() {
	// Create a TileDB context.
	ctx, err := tiledb.NewContext(nil)
	checkError(err)

	// The array will be 4x4 with dimensions "rows" and "cols",
	// with domain [1,4].
	domain, err := tiledb.NewDomain(ctx)
	checkError(err)
	dDim, err := tiledb.NewDimension(ctx, "d", []int32{1, 4}, int32(4))
	checkError(err)
	err = domain.AddDimensions(dDim)
	checkError(err)

	// The array will be sparse.
	schema, err := tiledb.NewArraySchema(ctx, tiledb.TILEDB_SPARSE)
	checkError(err)
	err = schema.SetDomain(domain)
	checkError(err)

	// Add a single attribute "a" so each (i) cell can store an integer.
	a, err := tiledb.NewAttribute(ctx, "a", tiledb.TILEDB_INT32)
	checkError(err)
	err = schema.AddAttributes(a)
	checkError(err)

	// Create the (empty) array on disk.
	array, err := tiledb.NewArray(ctx, vaccuumSparseArrayName)
	checkError(err)
	err = array.Create(schema)
	checkError(err)
}

func writeVacuumSparseArray(buffD []int32, data []int32) {
	ctx, err := tiledb.NewContext(nil)
	checkError(err)

	// Open the array for writing and create the query.
	array, err := tiledb.NewArray(ctx, vaccuumSparseArrayName)
	checkError(err)
	err = array.Open(tiledb.TILEDB_WRITE)
	checkError(err)
	query, err := tiledb.NewQuery(ctx, array)
	checkError(err)
	err = query.SetLayout(tiledb.TILEDB_UNORDERED)
	checkError(err)
	_, err = query.SetBuffer("d", buffD)
	checkError(err)
	_, err = query.SetBuffer("a", data)
	checkError(err)

	// Perform the write
	err = query.Submit()
	checkError(err)
	err = query.Finalize()
	checkError(err)
	err = array.Close()
	checkError(err)
}

func readVacuumSparseArray() {
	ctx, err := tiledb.NewContext(nil)
	checkError(err)

	// Prepare the array for reading
	array, err := tiledb.NewArray(ctx, vaccuumSparseArrayName)
	checkError(err)
	err = array.Open(tiledb.TILEDB_READ)
	checkError(err)

	buffD := make([]int32, 3)
	buffA := make([]int32, 3)

	// Prepare the query
	query, err := tiledb.NewQuery(ctx, array)
	checkError(err)
	_, err = query.SetBuffer("d", buffD)
	checkError(err)
	_, err = query.SetBuffer("a", buffA)
	checkError(err)
	err = query.SetLayout(tiledb.TILEDB_UNORDERED)
	checkError(err)
	err = query.AddRange(0, int32(1), int32(3))
	checkError(err)

	size, err := query.EstResultSize("a")
	fmt.Printf("Estimated query size in bytes: %d\n", *size)

	// Submit the query
	err = query.Submit()
	checkError(err)

	for i, aVal := range buffA {
		fmt.Printf("Cell (%d) has data %d\n", buffD[i], aVal)
	}

	err = query.Finalize()
	checkError(err)

	err = array.Close()
	checkError(err)
}

func numFragments() int {
	// Create a TileDB context.
	ctx, err := tiledb.NewContext(nil)
	checkError(err)

	// Create config object
	config, err := tiledb.NewConfig()
	checkError(err)

	// Create TileDB VFS.
	vfs, err := tiledb.NewVFS(ctx, config)
	checkError(err)

	num, err := vfs.NumOfFragmentsInPath(vaccuumSparseArrayName)
	checkError(err)

	return num
}

func consolidateVacuum() {
	// Write some simple data to cells (1, 2)
	buffD := []int32{1, 2}
	data := []int32{1, 2}
	writeVacuumSparseArray(buffD, data)

	// Write some simple data to cell (3)
	buffD = []int32{3}
	data = []int32{3}
	writeVacuumSparseArray(buffD, data)

	readVacuumSparseArray()

	ctx, err := tiledb.NewContext(nil)
	checkError(err)

	// Prepare the array for reading
	array, err := tiledb.NewArray(ctx, vaccuumSparseArrayName)
	checkError(err)
	err = array.Open(tiledb.TILEDB_WRITE)
	checkError(err)

	numOfFragments := numFragments()
	fmt.Printf("Num of fragments after 2 writes before consolidate: %d\n", numOfFragments)

	config, err := tiledb.NewConfig()
	checkError(err)

	err = config.Set("sm.consolidation.buffer_size", "4")
	checkError(err)

	err = array.Consolidate(config)
	checkError(err)

	numOfFragments = numFragments()
	fmt.Printf("Num of fragments after consolidate: %d\n", numOfFragments)

	err = array.Vacuum(config)
	checkError(err)

	numOfFragments = numFragments()
	fmt.Printf("Num of fragments after vacuum: %d\n", numOfFragments)

	readVacuumSparseArray()

	err = array.Close()
	checkError(err)
}

// ExampleVacuumSparseArray shows ysage of array vacuum function
func ExampleVacuumSparseArray() {
	createVacuumSparseArray()
	consolidateVacuum()

	// Cleanup example so unit tests are clean
	if _, err := os.Stat(vaccuumSparseArrayName); err == nil {
		err = os.RemoveAll(vaccuumSparseArrayName)
		checkError(err)
	}

	// Output: Estimated query size in bytes: 12
	// Cell (1) has data 1
	// Cell (2) has data 2
	// Cell (3) has data 3
	// Num of fragments after 2 writes before consolidate: 2
	// Num of fragments after consolidate: 3
	// Num of fragments after vacuum: 1
	// Estimated query size in bytes: 12
	// Cell (1) has data 1
	// Cell (2) has data 2
	// Cell (3) has data 3
}
