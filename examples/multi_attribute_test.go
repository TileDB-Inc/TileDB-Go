/**
 * @file   multi_attribute_test.go
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
 * This is a part of the TileDB "Multi-attribute Arrays" tutorial:
 *   https://docs.tiledb.io/en/latest/tutorials/multi-attribute-arrays.html
 *
 * When run, this program will create a simple 2D dense array with two
 * attributes, write some data to it, and read a slice of the data back on
 * (i) both attributes, and (ii) subselecting on only one of the attributes.
 *
 */

package examples

import (
	"fmt"
	"os"

	tiledb "github.com/TileDB-Inc/TileDB-Go"
)

// Name of array.
var multiAttributeArrayName = "multi_attribute_array"

func createMultiAttributeArray() {
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
	err = schema.SetDomain(domain)
	checkError(err)
	err = schema.SetCellOrder(tiledb.TILEDB_ROW_MAJOR)
	checkError(err)
	err = schema.SetTileOrder(tiledb.TILEDB_ROW_MAJOR)
	checkError(err)

	// Create two attributes "a1" and "a2", so each (i,j) cell can store
	// a character on "a1" and a vector of two floats on "a2".
	a1, err := tiledb.NewAttribute(ctx, "a1", tiledb.TILEDB_STRING_ASCII)
	checkError(err)
	a2, err := tiledb.NewAttribute(ctx, "a2", tiledb.TILEDB_FLOAT32)
	checkError(err)
	err = schema.AddAttributes(a1)
	checkError(err)
	err = a2.SetCellValNum(2)
	checkError(err)
	err = schema.AddAttributes(a2)
	checkError(err)

	// Create the (empty) array on disk.
	array, err := tiledb.NewArray(ctx, multiAttributeArrayName)
	checkError(err)
	err = array.Create(schema)
	checkError(err)
}

func writeMultiAttributeArray() {
	ctx, err := tiledb.NewContext(nil)
	checkError(err)

	// Prepare some data for the array
	a1 := []byte{'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l',
		'm', 'n', 'o', 'p'}
	a2 := []float32{1.1, 1.2, 2.1, 2.2, 3.1, 3.2, 4.1, 4.2,
		5.1, 5.2, 6.1, 6.2, 7.1, 7.2, 8.1, 8.2,
		9.1, 9.2, 10.1, 10.2, 11.1, 11.2, 12.1, 12.2,
		13.1, 13.2, 14.1, 14.2, 15.1, 15.2, 16.1, 16.2}

	// Create the query
	array, err := tiledb.NewArray(ctx, multiAttributeArrayName)
	checkError(err)
	err = array.Open(tiledb.TILEDB_WRITE)
	checkError(err)
	query, err := tiledb.NewQuery(ctx, array)
	checkError(err)
	err = query.SetLayout(tiledb.TILEDB_ROW_MAJOR)
	checkError(err)
	_, err = query.SetBuffer("a1", a1)
	checkError(err)
	_, err = query.SetBuffer("a2", a2)
	checkError(err)

	// Perform the write and close the array.
	err = query.Submit()
	checkError(err)
	err = array.Close()
	checkError(err)
}

func readMultiAttributeArray() {
	// Create TileDB context
	ctx, err := tiledb.NewContext(nil)
	checkError(err)

	// Prepare the array for reading
	array, err := tiledb.NewArray(ctx, multiAttributeArrayName)
	checkError(err)
	err = array.Open(tiledb.TILEDB_READ)
	checkError(err)

	// Slice only rows 1, 2 and cols 2, 3, 4
	subArray := []int32{1, 2, 2, 4}

	// Prepare the query
	query, err := tiledb.NewQuery(ctx, array)
	checkError(err)
	err = query.SetSubArray(subArray)
	checkError(err)

	// Prepare the vector that will hold the result
	// (of size 6 elements for "a1" and 12 elements for "a2" since
	// it stores two floats per cell)
	bufferElements, err := query.EstimateBufferElements()
	checkError(err)
	a1Data := make([]byte, bufferElements["a1"][1])
	a2Data := make([]float32, bufferElements["a2"][1])

	err = query.SetLayout(tiledb.TILEDB_ROW_MAJOR)
	checkError(err)
	_, err = query.SetBuffer("a1", a1Data)
	checkError(err)
	_, err = query.SetBuffer("a2", a2Data)
	checkError(err)

	// Submit the query and close the array.
	err = query.Submit()
	checkError(err)
	err = array.Close()
	checkError(err)

	fmt.Println("Reading both attributes a1 and a2:")
	for i := 0; i < int(bufferElements["a1"][1]); i++ {
		fmt.Printf("a1: %s, a2: (%.1f,%.1f)\n", string(a1Data[i]),
			a2Data[2*i], a2Data[2*i+1])
	}
	fmt.Printf("\n")
}

func readMultiAttributeArraySubSelect() {
	// Create TileDB context
	ctx, err := tiledb.NewContext(nil)
	checkError(err)

	// Prepare the array for reading
	array, err := tiledb.NewArray(ctx, multiAttributeArrayName)
	checkError(err)
	err = array.Open(tiledb.TILEDB_READ)
	checkError(err)

	// Slice only rows 1, 2 and cols 2, 3, 4
	subArray := []int32{1, 2, 2, 4}

	// Prepare the query
	query, err := tiledb.NewQuery(ctx, array)
	checkError(err)
	err = query.SetSubArray(subArray)
	checkError(err)

	// Prepare the vector that will hold the result
	// (of size 6 elements for "a1")
	bufferElements, err := query.EstimateBufferElements()
	checkError(err)
	a1Data := make([]byte, bufferElements["a1"][1])

	err = query.SetLayout(tiledb.TILEDB_ROW_MAJOR)
	checkError(err)
	_, err = query.SetBuffer("a1", a1Data)
	checkError(err)

	// Submit the query and close the array.
	err = query.Submit()
	checkError(err)
	err = array.Close()
	checkError(err)

	fmt.Println("Subselecting on attribute a1:")
	for i := 0; i < int(bufferElements["a1"][1]); i++ {
		fmt.Printf("a1: %s\n", string(a1Data[i]))
	}
	fmt.Printf("\n")
}

func ExampleMultiAttributeArray() {
	createMultiAttributeArray()
	writeMultiAttributeArray()
	readMultiAttributeArray()
	readMultiAttributeArraySubSelect()

	// Cleanup example so unit tests are clean
	if _, err := os.Stat(multiAttributeArrayName); err == nil {
		err = os.RemoveAll(multiAttributeArrayName)
		checkError(err)
	}

	// Output: Reading both attributes a1 and a2:
	// a1: b, a2: (2.1,2.2)
	// a1: c, a2: (3.1,3.2)
	// a1: d, a2: (4.1,4.2)
	// a1: f, a2: (6.1,6.2)
	// a1: g, a2: (7.1,7.2)
	// a1: h, a2: (8.1,8.2)
	//
	// Subselecting on attribute a1:
	// a1: b
	// a1: c
	// a1: d
	// a1: f
	// a1: g
	// a1: h
}
