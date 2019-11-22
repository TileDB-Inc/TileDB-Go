/**
 * @file   array_metadata_test.go
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2019 TileDB, Inc.
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
 * This is a part of the TileDB array metadata tutorial:
 * 	 https://docs.tiledb.io/en/latest/array_metadata.html
 *
 * When run, this program will create a simple 2D dense array, write some data
 * to it, and read a slice of the data back in the layout of the user's choice
 * (passed as an argument to the program: "row", "col", or "global").
 *
 */

package examples

import (
	"encoding/json"
	"fmt"
	"os"

	tiledb "github.com/TileDB-Inc/TileDB-Go"
)

// Name of array.
var arrayMetadataArrayName = "metadata_array"

func createArrayMetadataArray() {
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

	// Add a single attribute "a" so each (i,j) cell can store an integer.
	a, err := tiledb.NewAttribute(ctx, "a", tiledb.TILEDB_UINT32)
	checkError(err)
	err = schema.AddAttributes(a)
	checkError(err)

	// Create the (empty) array on disk.
	array, err := tiledb.NewArray(ctx, arrayMetadataArrayName)
	checkError(err)
	err = array.Create(schema)
	checkError(err)
}

func writeArrayMetadata() {
	ctx, err := tiledb.NewContext(nil)
	checkError(err)

	// Open the array for writing and create the query.
	array, err := tiledb.NewArray(ctx, arrayMetadataArrayName)
	checkError(err)
	err = array.Open(tiledb.TILEDB_WRITE)
	checkError(err)

	err = array.PutMetadata("key1", int32(25))
	checkError(err)

	err = array.PutMetadata("key2", []int32{25, 26, 27, 28})
	checkError(err)

	err = array.PutMetadata("key3", float32(25.1))
	checkError(err)

	err = array.PutMetadata("key4", []float32{25.1, 26.2, 27.3, 28.4})
	checkError(err)

	err = array.PutMetadata("key5", "This is TileDb array metadata")
	checkError(err)

	array.Free()
	ctx.Free()
}

func readArrayMetadata() {
	ctx, err := tiledb.NewContext(nil)
	checkError(err)

	// Prepare the array for reading
	array, err := tiledb.NewArray(ctx, arrayMetadataArrayName)
	checkError(err)
	err = array.Open(tiledb.TILEDB_READ)
	checkError(err)

	dataType, valueNum, value, err := array.GetMetadata("key1")
	checkError(err)

	fmt.Printf("Datatype: %d\n", dataType)
	fmt.Printf("Value Num: %d\n", valueNum)
	fmt.Printf("Value: %v\n", value.(int32))

	dataType, valueNum, value, err = array.GetMetadata("key2")
	checkError(err)

	fmt.Printf("Datatype: %d\n", dataType)
	fmt.Printf("Value Num: %d\n", valueNum)
	fmt.Printf("Value: %v\n", value.([]int32))

	dataType, valueNum, value, err = array.GetMetadata("key3")
	checkError(err)

	fmt.Printf("Datatype: %d\n", dataType)
	fmt.Printf("Value Num: %d\n", valueNum)
	fmt.Printf("Value: %v\n", value.(float32))

	dataType, valueNum, value, err = array.GetMetadata("key4")
	checkError(err)

	fmt.Printf("Datatype: %d\n", dataType)
	fmt.Printf("Value Num: %d\n", valueNum)
	fmt.Printf("Value: %v\n", value.([]float32))

	dataType, valueNum, value, err = array.GetMetadata("key5")
	checkError(err)

	// String can be retrieved:
	fmt.Printf("Value: %v\n", value.(string))

	numOfMetadata, err := array.GetMetadataNum()
	checkError(err)

	fmt.Printf("Num of metadata: %d\n", numOfMetadata)

	arrayMetadata, err := array.GetMetadataFromIndex(0)
	checkError(err)

	fmt.Printf("Key: %s\n", arrayMetadata.Key)
	fmt.Printf("Key len: %d\n", arrayMetadata.KeyLen)
	fmt.Printf("Datatype: %d\n", arrayMetadata.Datatype)
	fmt.Printf("Value Num: %d\n", arrayMetadata.ValueNum)
	fmt.Printf("Value: %v\n", arrayMetadata.Value.(int32))

	err = array.ConsolidateMetadata(nil)
	checkError(err)

	metadataMap, err := array.GetMetadataMap()
	checkError(err)

	jsonData, err := json.Marshal(metadataMap)
	checkError(err)

	fmt.Println(string(jsonData))

	array.Free()
	ctx.Free()
}

func clearArrayMetadata() {
	ctx, err := tiledb.NewContext(nil)
	checkError(err)

	// Prepare the array for writing
	array, err := tiledb.NewArray(ctx, arrayMetadataArrayName)
	checkError(err)
	err = array.Open(tiledb.TILEDB_WRITE)
	checkError(err)

	err = array.DeleteMetadata("key1")
	checkError(err)

	err = array.DeleteMetadata("key2")
	checkError(err)

	err = array.DeleteMetadata("key3")
	checkError(err)

	err = array.DeleteMetadata("key4")
	checkError(err)

	err = array.DeleteMetadata("key5")
	checkError(err)

	// Key does not exist
	err = array.DeleteMetadata("key6")
	checkError(err)

	array.Free()
	ctx.Free()
}

// ExampleArrayMetadataArray shows and example creation, writing and reading of a
// sparse array
func ExampleArrayMetadataArray() {
	createArrayMetadataArray()
	writeArrayMetadata()
	readArrayMetadata()
	clearArrayMetadata()

	// Cleanup example so unit tests are clean
	if _, err := os.Stat(arrayMetadataArrayName); err == nil {
		err = os.RemoveAll(arrayMetadataArrayName)
		checkError(err)
	}

	// Output: Datatype: 0
	// Value Num: 1
	// Value: 25
	// Datatype: 0
	// Value Num: 4
	// Value: [25 26 27 28]
	// Datatype: 2
	// Value Num: 1
	// Value: 25.1
	// Datatype: 2
	// Value Num: 4
	// Value: [25.1 26.2 27.3 28.4]
	// Value: This is TileDb array metadata
	// Num of metadata: 5
	// Key: key1
	// Key len: 4
	// Datatype: 0
	// Value Num: 1
	// Value: 25
	// {"key1":25,"key2":[25,26,27,28],"key3":25.1,"key4":[25.1,26.2,27.3,28.4],"key5":"This is TileDb array metadata"}
}
