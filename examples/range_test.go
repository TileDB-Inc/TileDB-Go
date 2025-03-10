/**
 * @file   range_test.go
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
 * This is a part of the TileDB range tutorial:
 *   https://docs.tiledb.io/en/latest/range.html
 *
 * When run, this program will create a simple 2D dense array, write some data
 * to it, and read a slice of the data back in the layout of the user's choice
 * (passed as an argument to the program: "row", "col", or "global").
 *
 */

package examples

import "github.com/TileDB-Inc/TileDB-Go/examples_lib"

// ExampleRunRange shows an example of creation, writing of a dense array
// and usage of range functions
func ExampleRunRange() {
	examples_lib.RunRange()

	// Error adding query range: [TileDB::Dimension] Error: Cannot add range to dimension; Lower range bound 1065353216 cannot be larger than the higher bound 4
	// Error adding query range: [TileDB::Subarray] Error: Cannot add range; Invalid dimension index
	// Number of ranges across dimension 0 is: 1
	// Number of ranges across dimension `rows` is: 1
	// Range start for dimension 0, range 0 is: 1
	// Range end for dimension 0, range 0 is: 4
}
