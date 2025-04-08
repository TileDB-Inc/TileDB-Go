/**
 * @file   string_dim_test.go
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
 * When run, this program will create a 2D sparse array, having string dim
 * write some data to it, and read a ranges of data to prove usability
 *
 */

package examples

import "github.com/TileDB-Inc/TileDB-Go/examples_lib"

// ExampleRunStringDimArray shows an example of creation, writing and reading of a
// sparse array with string dim
func ExampleRunStringDimArray() {
	examples_lib.RunStringDimArray()

	// Output: NonEmptyDomain Dimension Name: d
	// NonEmptyDomain Bounds: [aa dddd]
	// NonEmptyDomain Dimension Name: d
	// NonEmptyDomain Bounds: [aa dddd]
	// offsets: [0 2 4 6]
	// data: aabbccdddd
}
