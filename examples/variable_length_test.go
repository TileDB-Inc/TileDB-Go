/**
 * @file   variable_length_test.go
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
 *   https://docs.tiledb.io/en/latest/tutorials/variable-length-attributes.html
 *
 * When run, this program will create a simple 2D dense array with two
 * variable-length attributes, write some data to it, and read a slice of the
 * data back on both attributes.
 *
 */

package examples

import (
	"fmt"
	"github.com/TileDB-Inc/TileDB-Go"
	"os"
	"unsafe"
)

// Name of array.
var variableLengthArrayName = "variable_length_array"

func createVariableLengthArray() {
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

	// Add two variable-length attributes "a1" and "a2", the first storing
	// strings and the second storing a variable number of integers.
	a1, err := tiledb.NewAttribute(ctx, "a1", tiledb.TILEDB_STRING_ASCII)
	checkError(err)
	a2, err := tiledb.NewAttribute(ctx, "a2", tiledb.TILEDB_INT32)
	checkError(err)
	err = a1.SetCellValNum(tiledb.TILEDB_VAR_NUM)
	checkError(err)
	err = schema.AddAttributes(a1)
	checkError(err)
	err = a2.SetCellValNum(tiledb.TILEDB_VAR_NUM)
	checkError(err)
	err = schema.AddAttributes(a2)
	checkError(err)

	// Create the (empty) array on disk.
	array, err := tiledb.NewArray(ctx, variableLengthArrayName)
	checkError(err)
	err = array.Create(schema)
	checkError(err)
}

func writeVariableLengthArray() {
	ctx, err := tiledb.NewContext(nil)
	checkError(err)

	// Prepare some data for the array
	a1Data := []byte("a" + "bb" + "ccc" + "dd" + "eee" + "f" + "g" + "hhh" +
		"i" + "jjj" + "kk" + "l" + "m" + "n" + "oo" + "p")
	a1Off := []uint64{
		0, 1, 3, 6, 8, 11, 12, 13, 16, 17, 20, 22, 23, 24, 25, 27}
	a2Data := []int32{
		1, 1, 2, 2, 3, 4, 5, 6, 6, 7, 7, 8, 8,
		8, 9, 9, 10, 11, 12, 12, 13, 14, 14, 14, 15, 16}
	a2ElOff := []uint64{
		0, 2, 4, 5, 6, 7, 9, 11, 14, 16, 17, 18, 20, 21, 24, 25}

	a2Off := make([]uint64, 16)
	for i := range a2ElOff {
		a2Off[i] = a2ElOff[i] * uint64(unsafe.Sizeof(int32(0)))
	}

	// Open the array for writing and create the query.
	array, err := tiledb.NewArray(ctx, variableLengthArrayName)
	checkError(err)
	err = array.Open(tiledb.TILEDB_WRITE)
	checkError(err)
	query, err := tiledb.NewQuery(ctx, array)
	checkError(err)
	err = query.SetLayout(tiledb.TILEDB_ROW_MAJOR)
	checkError(err)
	_, _, err = query.SetBufferVar("a1", a1Off, a1Data)
	checkError(err)
	_, _, err = query.SetBufferVar("a2", a2Off, a2Data)
	checkError(err)

	// Perform the write and close the array.
	err = query.Submit()
	checkError(err)
	err = array.Close()
	checkError(err)
}

func printResultsVariableLength(
	a1Off []uint64,
	a1Data []byte,
	a2Off []uint64,
	a2Data []int32,
	resultElMap map[string][2]uint64) {

	// Get the string sizes
	resultElA1Off := resultElMap["a1"][0]

	var a1StrSizes []uint64
	for i := 0; i < int(resultElA1Off)-1; i++ {
		a1StrSizes = append(a1StrSizes, a1Off[i+1]-a1Off[i])
	}

	resultA1DataSize := resultElMap["a1"][1] *
		uint64(unsafe.Sizeof(byte(0)))
	a1StrSizes = append(a1StrSizes,
		resultA1DataSize-a1Off[resultElA1Off-1])

	// Get the strings
	a1Str := make([][]byte, resultElA1Off)
	for i := 0; i < int(resultElA1Off); i++ {
		a1Str[i] = make([]byte, 0)
		for j := 0; j < int(a1StrSizes[i]); j++ {
			a1Str[i] = append(a1Str[i], a1Data[a1Off[i]])
		}
	}

	// Get the element offsets
	var a2ElOff []uint64
	resultElA2Off := resultElMap["a2"][0]
	for i := 0; i < int(resultElA2Off); i++ {
		a2ElOff = append(a2ElOff, a2Off[i]/uint64(unsafe.Sizeof(int32(0))))
	}

	// Get the number of elements per cell value
	var a2CellEl []uint64
	for i := 0; i < int(resultElA2Off)-1; i++ {
		a2CellEl = append(a2CellEl, a2ElOff[i+1]-a2ElOff[i])
	}
	resultElA2Data := resultElMap["a2"][1]
	a2CellEl = append(a2CellEl, resultElA2Data-a2ElOff[len(a2ElOff)-1])

	// Print the results
	for i := 0; i < int(resultElA1Off); i++ {
		fmt.Printf("a1: %s, a2: ", string(a1Str[i]))
		for j := 0; j < int(a2CellEl[i]); j++ {
			fmt.Printf("%d", a2Data[a2ElOff[i]+uint64(j)])
		}
		fmt.Printf("\n")
	}
}

func readVariableLengthArray() {
	// Create TileDB context
	ctx, err := tiledb.NewContext(nil)
	checkError(err)

	// Prepare the array for reading
	array, err := tiledb.NewArray(ctx, variableLengthArrayName)
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

	maxElMap, err := array.MaxBufferElements(subArray)
	checkError(err)

	a1Off := make([]uint64, maxElMap["a1"][0])
	a1Data := make([]byte, maxElMap["a1"][1])
	a2Off := make([]uint64, maxElMap["a2"][0])
	a2Data := make([]int32, maxElMap["a2"][1])

	err = query.SetLayout(tiledb.TILEDB_ROW_MAJOR)
	checkError(err)
	_, _, err = query.SetBufferVar("a1", a1Off, a1Data)
	checkError(err)
	_, _, err = query.SetBufferVar("a2", a2Off, a2Data)
	checkError(err)

	// Submit the query and close the array.
	err = query.Submit()
	checkError(err)

	elements, err := query.ResultBufferElements()
	checkError(err)
	printResultsVariableLength(a1Off, a1Data, a2Off, a2Data, elements)

	err = array.Close()
	checkError(err)
}

func ExampleVariableLengthArray() {
	createVariableLengthArray()
	writeVariableLengthArray()
	readVariableLengthArray()

	// Cleanup example so unit tests are clean
	if _, err := os.Stat(variableLengthArrayName); err == nil {
		err = os.RemoveAll(variableLengthArrayName)
		checkError(err)
	}

	// Output: a1: bb, a2: 22
	// a1: ccc, a2: 3
	// a1: dd, a2: 4
	// a1: f, a2: 66
	// a1: g, a2: 77
	// a1: hhh, a2: 888
}
