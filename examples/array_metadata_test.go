/**
 * @file   array_metadata_test.go
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2021 TileDB, Inc.
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
	"github.com/TileDB-Inc/TileDB-Go/examples_lib"
)

// ExampleRunArrayMetadataArray shows and example creation, writing and reading of a
// sparse array
func ExampleRunArrayMetadataArray() {
	examples_lib.RunArrayMetadataArray()

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
	// Num of metadata: 6
	// Key: key1
	// Key len: 4
	// Datatype: 0
	// Value Num: 1
	// Value: 25
	// Key: key6
	// Key len: 4
	// Datatype: 4
	// Value Num: 1
	// Value: Thi
	// {"key1":25,"key2":[25,26,27,28],"key3":25.1,"key4":[25.1,26.2,27.3,28.4],"key5":"This is TileDb array metadata","key6":"This is TileDb array char metadata"}
}
