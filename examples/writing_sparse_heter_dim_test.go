/**
 * @file   writing_sparse_heter_dim_test.go
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
 * When run, this program will create a simple 2D sparse array, having
 * heterogeneous dimensions and writes data to it
 *
 */

package examples

import (
	"fmt"
	"os"

	tiledb "github.com/TileDB-Inc/TileDB-Go"
)

// Name of array.
var heterArrayName = "writing_sparse_heter_dim"

func createSparseHeterDimArray() {
	// Create a TileDB context.
	ctx, err := tiledb.NewContext(nil)
	checkError(err)

	// The array will be 4x4 with dimensions "rows" and "cols",
	// with domain [1,4].
	domain, err := tiledb.NewDomain(ctx)
	checkError(err)
	d1, err := tiledb.NewDimension(ctx, "d1", []float32{1.0, 20.0}, float32(5.0))
	checkError(err)
	d2, err := tiledb.NewDimension(ctx, "d2", []int64{1, 30}, int64(5))
	checkError(err)
	err = domain.AddDimensions(d1, d2)
	checkError(err)

	// The array will be sparse.
	schema, err := tiledb.NewArraySchema(ctx, tiledb.TILEDB_SPARSE)
	checkError(err)
	err = schema.SetDomain(domain)
	checkError(err)

	// Add a single attribute "a" so each (i,j) cell can store an integer.
	a, err := tiledb.NewAttribute(ctx, "a", tiledb.TILEDB_INT32)
	checkError(err)
	err = schema.AddAttributes(a)
	checkError(err)

	// Create the (empty) array on disk.
	array, err := tiledb.NewArray(ctx, heterArrayName)
	checkError(err)
	err = array.Create(schema)
	checkError(err)
}

func writeSparseHeterDimArray() {
	ctx, err := tiledb.NewContext(nil)
	checkError(err)

	// Write some simple data to cells.
	buffD1 := []float32{1.1, 1.2, 1.3, 1.4}
	buffD2 := []int64{1, 2, 3, 4}
	buffA := []int32{1, 2, 3, 4}

	// Open the array for writing and create the query.
	array, err := tiledb.NewArray(ctx, heterArrayName)
	checkError(err)
	err = array.Open(tiledb.TILEDB_WRITE)
	checkError(err)
	query, err := tiledb.NewQuery(ctx, array)
	checkError(err)
	err = query.SetLayout(tiledb.TILEDB_UNORDERED)
	checkError(err)
	_, err = query.SetBuffer("d1", buffD1)
	checkError(err)
	_, err = query.SetBuffer("d2", buffD2)
	checkError(err)
	_, err = query.SetBuffer("a", buffA)
	checkError(err)

	// Perform the write and close the array.
	err = query.Submit()
	checkError(err)
	err = array.Close()
	checkError(err)
}

func readSparseHeterDimArray() {
	ctx, err := tiledb.NewContext(nil)
	checkError(err)

	// Prepare the array for reading
	array, err := tiledb.NewArray(ctx, heterArrayName)
	checkError(err)
	err = array.Open(tiledb.TILEDB_READ)
	checkError(err)

	buffD1R := make([]float32, 4)
	buffD2R := make([]int64, 4)
	buffAR := make([]int32, 4)

	// Prepare the query
	query, err := tiledb.NewQuery(ctx, array)
	checkError(err)
	_, err = query.SetBuffer("d1", buffD1R)
	checkError(err)
	_, err = query.SetBuffer("d2", buffD2R)
	checkError(err)
	_, err = query.SetBuffer("a", buffAR)
	checkError(err)
	err = query.SetLayout(tiledb.TILEDB_UNORDERED)
	checkError(err)
	err = query.AddRange(0, float32(1.0), float32(20.0))
	checkError(err)
	err = query.AddRange(1, int64(1), int64(30))
	checkError(err)

	// Submit the query and close the array.
	err = query.Submit()
	checkError(err)

	fmt.Printf("D1 Buffer: %v\n", buffD1R)
	fmt.Printf("D2 Buffer: %v\n", buffD2R)
	fmt.Printf("A Attribute Data: %v\n", buffAR)

	err = array.Close()
	checkError(err)
}

// ExampleSparseHeterDimArray shows and example creation, writing and reading of
// a sparse array using heterogeneus dimensions
func ExampleSparseHeterDimArray() {
	createSparseHeterDimArray()
	writeSparseHeterDimArray()
	readSparseHeterDimArray()

	// Cleanup example so unit tests are clean
	if _, err := os.Stat(heterArrayName); err == nil {
		err = os.RemoveAll(heterArrayName)
		checkError(err)
	}

	// Output: D1 Buffer: [1.1 1.2 1.3 1.4]
	// D2 Buffer: [1 2 3 4]
	// A Attribute Data: [1 2 3 4]
}
