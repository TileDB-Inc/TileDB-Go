/**
 * @file   reading_incomplete_test.go
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
 *   https://docs.tiledb.io/en/latest/tutorials/reading.html
 *
 * This example demonstrates the concept of incomplete read queries
 * for a sparse array with two attributes.
 */

package examples

import (
	"fmt"
	"os"
	"unsafe"

	"github.com/TileDB-Inc/TileDB-Go"
)

// Name of array.
var readingIncompleteArrayName = "reading_incomplete_array"

func createReadingIncompleteArray() {
	// Create a TileDB context.
	ctx, err := tiledb.NewContext(nil)
	checkError(err)

	// The array will be 4x4 with dimensions "rows" and "cols",
	// with domain [1,4].
	domain, err := tiledb.NewDomain(ctx)
	checkError(err)
	rowDim, err := tiledb.NewDimension(ctx, "rows", []int32{1, 4}, int32(2))
	checkError(err)
	colDim, err := tiledb.NewDimension(ctx, "cols", []int32{1, 4}, int32(2))
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

	// Add an attribute "a1" so each (i,j) cell can store an integer.
	a1, err := tiledb.NewAttribute(ctx, "a1", tiledb.TILEDB_INT32)
	checkError(err)
	err = schema.AddAttributes(a1)
	checkError(err)

	// Add an attribute "a2" so each (i,j) cell can store a string.
	a2, err := tiledb.NewAttribute(ctx, "a2", tiledb.TILEDB_STRING_UTF8)
	checkError(err)
	err = a2.SetCellValNum(tiledb.TILEDB_VAR_NUM)
	checkError(err)
	err = schema.AddAttributes(a2)
	checkError(err)

	// Create the (empty) array on disk.
	array, err := tiledb.NewArray(ctx, readingIncompleteArrayName)
	checkError(err)
	err = array.Create(schema)
	checkError(err)
}

func writeReadingIncompleteArray() {
	ctx, err := tiledb.NewContext(nil)
	checkError(err)

	// Prepare some data for the array
	coords := []int32{1, 1, 2, 1, 2, 2}
	a1Data := []int32{1, 2, 3}
	a2Data := []byte("abbccc")
	a2Off := []uint64{0, 1, 3}

	// Open the array for writing and create the query.
	array, err := tiledb.NewArray(ctx, readingIncompleteArrayName)
	checkError(err)
	err = array.Open(tiledb.TILEDB_WRITE)
	checkError(err)
	query, err := tiledb.NewQuery(ctx, array)
	checkError(err)
	err = query.SetLayout(tiledb.TILEDB_GLOBAL_ORDER)
	checkError(err)
	_, err = query.SetBuffer("a1", a1Data)
	checkError(err)
	_, _, err = query.SetBufferVar("a2", a2Off, a2Data)
	checkError(err)
	_, err = query.SetCoordinates(coords)
	checkError(err)

	// Perform the write, finalize and close the array.
	err = query.Submit()
	checkError(err)
	err = query.Finalize()
	checkError(err)
	err = array.Close()
	checkError(err)
}

func reallocateBuffers(
	coords *[]int32,
	a1Data *[]int32,
	a2Off *[]uint64,
	a2Data *[]byte) {
	fmt.Println("Reallocating...")

	//// Note: this is a naive reallocation - you should handle
	//// reallocation properly depending on your application
	*coords = make([]int32, 2*len(*coords))
	*a1Data = make([]int32, 2*len(*a1Data))
	*a2Off = make([]uint64, 2*len(*a2Off))
	*a2Data = make([]byte, 2*len(*a2Data))
}

func printResultsReadingIncomplete(
	coords []int32,
	a1Data []int32,
	a2Off []uint64,
	a2Data []byte,
	resultElMap map[string][2]uint64) {
	fmt.Println("Printing results...")

	// Get the string sizes
	resultElA2Off := resultElMap["a2"][0]

	var a2StrSizes []uint64

	for i := 0; i < int(resultElA2Off)-1; i++ {
		a2StrSizes = append(a2StrSizes, a2Off[i+1]-a2Off[i])
	}

	resultA2DataSize := resultElMap["a2"][1] *
		uint64(unsafe.Sizeof(byte(0)))
	a2StrSizes = append(a2StrSizes,
		resultA2DataSize-a2Off[resultElA2Off-1])

	// Get the strings
	a2Str := make([][]byte, resultElA2Off)
	for i := 0; i < int(resultElA2Off); i++ {
		a2Str[i] = make([]byte, 0)
		for j := 0; j < int(a2StrSizes[i]); j++ {
			a2Str[i] = append(a2Str[i], a2Data[a2Off[i]])
		}
	}

	// Print the results
	resultNum := resultElA2Off // For clarity
	for r := 0; r < int(resultNum); r++ {
		i := coords[2*r]
		j := coords[2*r+1]
		a1 := a1Data[r]
		fmt.Printf("Cell (%d, %d), a1: %d, a2: %s\n",
			i, j, a1, string(a2Str[r]))
	}
}

func readReadingIncompleteArray() {
	ctx, err := tiledb.NewContext(nil)
	checkError(err)

	// Prepare the array for reading
	array, err := tiledb.NewArray(ctx, readingIncompleteArrayName)
	checkError(err)
	err = array.Open(tiledb.TILEDB_READ)
	checkError(err)

	// Read the entire array
	subArray := []int32{1, 4, 1, 4}

	// Prepare buffers such that the results **cannot** fit
	coords := make([]int32, 2)
	a1Data := make([]int32, 1)
	a2Off := make([]uint64, 1)
	a2Data := make([]byte, 1)

	// Prepare the query
	query, err := tiledb.NewQuery(ctx, array)
	checkError(err)
	err = query.SetSubArray(subArray)
	checkError(err)
	err = query.SetLayout(tiledb.TILEDB_ROW_MAJOR)
	checkError(err)
	_, err = query.SetBuffer("a1", a1Data)
	checkError(err)
	_, _, err = query.SetBufferVar("a2", a2Off, a2Data)
	checkError(err)
	_, err = query.SetCoordinates(coords)
	checkError(err)

	var queryStatus tiledb.QueryStatus

	for i := 0; i < 5; i++ {
		// Submit the query
		err = query.Submit()
		checkError(err)

		queryStatus, err = query.Status()
		checkError(err)

		fmt.Println(queryStatus.String())

		// Print out the results.
		elements, err := query.ResultBufferElements()
		checkError(err)
		resultNum := elements["a1"][1]
		fmt.Printf("resultNum=%d\n", resultNum)

		hasResults, err := query.HasResults()
		checkError(err)
		fmt.Printf("hasResults=%v\n", hasResults)

		if queryStatus == tiledb.TILEDB_INCOMPLETE && !hasResults {
			reallocateBuffers(&coords, &a1Data, &a2Off, &a2Data)
			_, err = query.SetBuffer("a1", a1Data)
			checkError(err)
			_, _, err = query.SetBufferVar("a2", a2Off, a2Data)
			checkError(err)
			_, err = query.SetCoordinates(coords)
			checkError(err)
		} else {
			elements, err := query.ResultBufferElements()
			checkError(err)
			printResultsReadingIncomplete(
				coords, a1Data, a2Off, a2Data, elements)
		}

		if queryStatus != tiledb.TILEDB_INCOMPLETE {
			break
		}
	}

	err = array.Close()
	checkError(err)
}

func ExampleReadingIncompleteArray() {
	createReadingIncompleteArray()
	writeReadingIncompleteArray()
	readReadingIncompleteArray()

	// Cleanup example so unit tests are clean
	if _, err := os.Stat(readingIncompleteArrayName); err == nil {
		err = os.RemoveAll(readingIncompleteArrayName)
		checkError(err)
	}

	// Output: Printing results...
	// Cell (1, 1), a1: 1, a2: a
	// Reallocating...
	// Printing results...
	// Cell (2, 1), a1: 2, a2: bb
	// Reallocating...
	// Printing results...
	// Cell (2, 2), a1: 3, a2: ccc
}
