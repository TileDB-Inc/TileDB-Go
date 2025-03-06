/**
 * @file   variable_length_test.go
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
 * This is a part of the TileDB "Multi-attribute Arrays" tutorial:
 *   https://docs.tiledb.io/en/latest/tutorials/variable-length-attributes.html
 *
 * When run, this program will create a simple 2D dense array with two
 * variable-length attributes, write some data to it, and read a slice of the
 * data back on both attributes.
 *
 */

package examples

import "github.com/TileDB-Inc/TileDB-Go/examples_lib"

func ExampleRunVariableLengthArray() {
	examples_lib.RunVariableLengthArray()

	// Output:
	// a1, Estimated offset size: 48, estimated query size in bytes: 24
	// a2, Estimated offset size: 48, estimated query size in bytes: 39
	// a1: bb, a2: 22
	// a1: ccc, a2: 3
	// a1: dd, a2: 4
	// a1: f, a2: 66
	// a1: g, a2: 77
	// a1: hhh, a2: 888
}
