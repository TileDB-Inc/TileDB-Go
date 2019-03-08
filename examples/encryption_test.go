/**
 * @file   encryption_test.go
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
 * This is a part of the TileDB encryption tutorial:
 *   https://docs.tiledb.io/en/latest/tutorials/encryption.html
 *
 * When run, this program will create an encrypted 2D dense array, write some
 * data to it, and read a slice of the data back.
 *
 */

package examples

import (
	"fmt"
	"github.com/TileDB-Inc/TileDB-Go"
	"os"
)

// Name of array.
var encryptedArrayName = "encrypted_array"

// The 256-bit encryption key, stored as a string for convenience.
var encryption_key = "0123456789abcdeF0123456789abcdeF"

func createEncryptedArray() {
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

	// Add a single attribute "a" so each (i,j) cell can store an integer.
	a, err := tiledb.NewAttribute(ctx, "a", tiledb.TILEDB_INT32)
	checkError(err)
	err = schema.AddAttributes(a)
	checkError(err)

	// Create the (empty) encrypted array with AES-256-GCM.
	array, err := tiledb.NewArray(ctx, encryptedArrayName)
	checkError(err)
	err = array.CreateWithKey(schema, tiledb.TILEDB_AES_256_GCM, encryption_key)
	checkError(err)
}

func writeEncryptedArray() {
	ctx, err := tiledb.NewContext(nil)
	checkError(err)

	// Prepare some data for the array
	data := []int32{
		1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}

	// Open the array for writing and create the query.
	array, err := tiledb.NewArray(ctx, encryptedArrayName)
	checkError(err)
	err = array.OpenWithKey(tiledb.TILEDB_WRITE, tiledb.TILEDB_AES_256_GCM,
		encryption_key)
	checkError(err)
	query, err := tiledb.NewQuery(ctx, array)
	checkError(err)
	err = query.SetLayout(tiledb.TILEDB_ROW_MAJOR)
	checkError(err)
	_, err = query.SetBuffer("a", data)
	checkError(err)

	// Perform the write and close the array.
	err = query.Submit()
	checkError(err)
	err = array.Close()
	checkError(err)
}

func readEncryptedArray() {
	ctx, err := tiledb.NewContext(nil)
	checkError(err)

	// Prepare the array for reading
	array, err := tiledb.NewArray(ctx, encryptedArrayName)
	checkError(err)
	err = array.OpenWithKey(tiledb.TILEDB_READ, tiledb.TILEDB_AES_256_GCM,
		encryption_key)
	checkError(err)

	// Slice only rows 1, 2 and cols 2, 3, 4
	subArray := []int32{1, 2, 2, 4}

	// Prepare the vector that will hold the result (of size 6 elements)
	data := make([]int32, 6)

	// Prepare the query
	query, err := tiledb.NewQuery(ctx, array)
	checkError(err)
	err = query.SetSubArray(subArray)
	checkError(err)
	err = query.SetLayout(tiledb.TILEDB_ROW_MAJOR)
	checkError(err)
	_, err = query.SetBuffer("a", data)
	checkError(err)

	// Submit the query and close the array.
	err = query.Submit()
	checkError(err)
	err = array.Close()
	checkError(err)

	// Print out the results.
	fmt.Println(data)
}

func ExampleEncryptedArray() {
	createEncryptedArray()
	writeEncryptedArray()
	readEncryptedArray()

	// Cleanup example so unit tests are clean
	if _, err := os.Stat(encryptedArrayName); err == nil {
		err = os.RemoveAll(encryptedArrayName)
		checkError(err)
	}

	// Output: [2 3 4 6 7 8]
}
