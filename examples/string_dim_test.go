/**
 * @file   string_dim_test.go
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
 * When run, this program will create a 2D sparse array, having string dim
 * write some data to it, and read a ranges of data to prove usability
 *
 */

package examples

import (
	"fmt"
	"os"

	tiledb "github.com/TileDB-Inc/TileDB-Go"
)

// Name of array.
var stringDimArrayName = "string_dim"

func createStringDimArray() {
	// Create a TileDB context.
	ctx, err := tiledb.NewContext(nil)
	checkError(err)

	// The array will be 4x4 with dimensions "rows" and "cols",
	// with domain [1,4].
	domain, err := tiledb.NewDomain(ctx)
	checkError(err)
	d, err := tiledb.NewStringDimension(ctx, "d")
	checkError(err)
	err = domain.AddDimensions(d)
	checkError(err)

	// The array will be sparse.
	schema, err := tiledb.NewArraySchema(ctx, tiledb.TILEDB_SPARSE)
	checkError(err)
	err = schema.SetDomain(domain)
	checkError(err)

	// Add a single attribute "a" so each cell can store an integer.
	a, err := tiledb.NewAttribute(ctx, "a", tiledb.TILEDB_INT32)
	checkError(err)
	err = schema.AddAttributes(a)
	checkError(err)

	// Create the (empty) array on disk.
	array, err := tiledb.NewArray(ctx, stringDimArrayName)
	checkError(err)
	err = array.Create(schema)
	checkError(err)
}

func writeStringDimArray() {
	ctx, err := tiledb.NewContext(nil)
	checkError(err)

	// Open the array for writing
	array, err := tiledb.NewArray(ctx, stringDimArrayName)
	checkError(err)
	err = array.Open(tiledb.TILEDB_WRITE)
	checkError(err)

	// Prepare some data for the array
	buffA := []int32{3, 2, 1, 4}
	dData := []byte("ccbbddddaa")
	dOff := []uint64{0, 2, 4, 8}

	// Create the query
	query, err := tiledb.NewQuery(ctx, array)
	checkError(err)
	_, _, err = query.SetBufferVar("d", dOff, dData)
	checkError(err)
	_, err = query.SetBuffer("a", buffA)
	checkError(err)
	err = query.SetLayout(tiledb.TILEDB_UNORDERED)
	checkError(err)

	// Perform the write and close the array.
	err = query.Submit()
	checkError(err)
	err = array.Close()
	checkError(err)
}

func readStringDimArray() {
	ctx, err := tiledb.NewContext(nil)
	checkError(err)

	// Prepare the array for reading
	array, err := tiledb.NewArray(ctx, stringDimArrayName)
	checkError(err)
	err = array.Open(tiledb.TILEDB_READ)
	checkError(err)

	nonEmptyDomain, isEmpty, err := array.NonEmptyDomainVarFromName("d")
	checkError(err)

	if !isEmpty {
		fmt.Printf("NonEmptyDomain Dimension Name: %v\n", nonEmptyDomain.DimensionName)
		fmt.Printf("NonEmptyDomain Bounds: %v\n", nonEmptyDomain.Bounds)
	}

	// Prepare the query
	query, err := tiledb.NewQuery(ctx, array)
	checkError(err)
	s1 := "a"
	s2 := "ee"
	err = query.AddRangeVar(0, s1, s2)
	checkError(err)

	offsets := make([]uint64, 4)
	data := make([]byte, 10)
	_, _, err = query.SetBufferVar("d", offsets, data)
	checkError(err)

	// Submit the query and close the array.
	err = query.Submit()
	checkError(err)

	fmt.Printf("offsets: %v\n", offsets)
	fmt.Printf("data: %s\n", string(data))

	err = array.Close()
	checkError(err)
}

// ExampleStringDimArray shows an example of creation, writing and reading of a
// sparse array with string dim
func ExampleStringDimArray() {
	createStringDimArray()
	writeStringDimArray()
	readStringDimArray()

	// Cleanup example so unit tests are clean
	if _, err := os.Stat(stringDimArrayName); err == nil {
		err = os.RemoveAll(stringDimArrayName)
		checkError(err)
	}

	// Output: NonEmptyDomain Dimension Name: d
	// NonEmptyDomain Bounds: [aa dddd]
	// offsets: [0 2 4 6]
	// data: aabbccdddd
}
