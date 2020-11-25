/**
 * @file   reading_timestamp_test.go
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
 * This is a part of the TileDB quickstart tutorial:
 *   https://docs.tiledb.io/en/latest/quickstart.html
 *
 * When run, this program will create a 2D dense array, write data and metadata
 * to it multiple times keeping track of write timestamps, then read at specific
 * timestamp using array.OpenAt and prove correlation of timestamps to fragments
 * and metadata created /updated
 *
 */

package examples

import (
	"fmt"
	"os"
	"time"

	tiledb "github.com/TileDB-Inc/TileDB-Go"
)

// Name of array.
var timestampArrayName = "timestamp_metadata"

func createTimestampArray() {
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

	// Create the (empty) array on disk.
	array, err := tiledb.NewArray(ctx, timestampArrayName)
	checkError(err)
	err = array.Create(schema)
	checkError(err)
}

func writeTimestampArray(key string, value string, timestamp uint64, bias int32) {
	ctx, err := tiledb.NewContext(nil)
	checkError(err)

	// Prepare some data for the array
	data := []int32{
		1, 2 + bias, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}

	// Open the array for writing and create the query.
	array, err := tiledb.NewArray(ctx, timestampArrayName)
	checkError(err)
	err = array.OpenAt(tiledb.TILEDB_WRITE, timestamp)
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

	fmt.Printf("Writing %s: %s\n", key, value)
	err = array.PutMetadata(key, value)
	checkError(err)

	err = array.Close()
	checkError(err)
}

func writeTimestampArrayMeta(key string, value string, timestamp uint64) {
	ctx, err := tiledb.NewContext(nil)
	checkError(err)

	// Open the array for writing
	array, err := tiledb.NewArray(ctx, timestampArrayName)
	checkError(err)
	err = array.OpenAt(tiledb.TILEDB_WRITE, timestamp)
	checkError(err)

	fmt.Printf("Writing %s: %s\n", key, value)
	err = array.PutMetadata(key, value)
	checkError(err)

	err = array.Close()
	checkError(err)
}

func readTimestampArray(timestamp uint64) {
	ctx, err := tiledb.NewContext(nil)
	checkError(err)

	// Prepare the array for reading
	array, err := tiledb.NewArray(ctx, timestampArrayName)
	checkError(err)
	err = array.OpenAt(tiledb.TILEDB_READ, timestamp)
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

	_, _, value, err := array.GetMetadata("meta_key")
	checkError(err)

	// String can be retrieved:
	fmt.Printf("Value: %v\n", value.(string))

	err = array.Close()
	checkError(err)

	// Print out the results.
	fmt.Println(data)
}

func getTimestamp() uint64 {
	return uint64(time.Now().UTC().UnixNano() / 1000000)
}

// ExampleTimestampArray shows timestamp correlation of written data and metadata
func ExampleTimestampArray() {
	createTimestampArray()
	// Write data and metadata
	t1 := getTimestamp()
	writeTimestampArray("meta_key", "Write1", t1, 0)
	time.Sleep(2000 * time.Millisecond)
	// Write metadata only
	t2 := getTimestamp()
	writeTimestampArrayMeta("meta_key", "Write2", t2)
	time.Sleep(2000 * time.Millisecond)
	// Write metadata only
	t3 := getTimestamp()
	writeTimestampArrayMeta("meta_key", "Write3", t3)
	readTimestampArray(t1)
	readTimestampArray(t2)
	readTimestampArray(t3)

	if _, err := os.Stat(timestampArrayName); err == nil {
		err = os.RemoveAll(timestampArrayName)
		checkError(err)
	}

	// Writing data and metadata
	createTimestampArray()
	t1 = getTimestamp()
	writeTimestampArray("meta_key", "Write1", t1, 0)
	time.Sleep(2000 * time.Millisecond)
	t2 = getTimestamp()
	writeTimestampArray("meta_key", "Write2", t2, 1)
	time.Sleep(2000 * time.Millisecond)
	t3 = getTimestamp()
	writeTimestampArray("meta_key", "Write3", t3, 2)
	readTimestampArray(t1)
	readTimestampArray(t2)
	readTimestampArray(t3)

	// Cleanup example so unit tests are clean
	if _, err := os.Stat(timestampArrayName); err == nil {
		err = os.RemoveAll(timestampArrayName)
		checkError(err)
	}

	// Output: Writing meta_key: Write1
	// Writing meta_key: Write2
	// Writing meta_key: Write3
	// Value: Write1
	// [2 3 4 6 7 8]
	// Value: Write2
	// [2 3 4 6 7 8]
	// Value: Write3
	// [2 3 4 6 7 8]
	// Writing meta_key: Write1
	// Writing meta_key: Write2
	// Writing meta_key: Write3
	// Value: Write1
	// [2 3 4 6 7 8]
	// Value: Write2
	// [3 3 4 6 7 8]
	// Value: Write3
	// [4 3 4 6 7 8]
}
