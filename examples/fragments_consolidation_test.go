/**
 * @file   fragments_consolidation_test.go
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
 * This is a part of the TileDB tutorial:
 *   https://docs.tiledb.io/en/latest/tutorials/fragments-consolidation.html
 *
 * When run, this program will create a simple 2D dense array, write some data
 * with three queries (creating three fragments), optionally consolidate
 * and read the entire array data back.
 */

package examples

import "github.com/TileDB-Inc/TileDB-Go/examples_lib"

func ExampleFragmentsConsolidationArray() {
	examples_lib.RunFragmentsConsolidationArray()

	// Output: Num of fragments: 1
	// Cell (1, 1) has data 201
	// Cell (1, 2) has data 2
	// Cell (1, 3) has data 3
	// Cell (1, 4) has data 4
	// Cell (2, 1) has data 5
	// Cell (2, 2) has data 101
	// Cell (2, 3) has data 102
	// Cell (2, 4) has data 8
	// Cell (3, 1) has data -2147483648
	// Cell (3, 2) has data 103
	// Cell (3, 3) has data 104
	// Cell (3, 4) has data 202
	// Cell (4, 1) has data -2147483648
	// Cell (4, 2) has data -2147483648
	// Cell (4, 3) has data -2147483648
	// Cell (4, 4) has data -2147483648
}
