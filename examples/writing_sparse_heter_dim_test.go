/**
 * @file   writing_sparse_heter_dim_test.go
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
 * When run, this program will create a simple 2D sparse array, having
 * heterogeneous dimensions and writes data to it
 *
 */

package examples

import "github.com/TileDB-Inc/TileDB-Go/examples_lib"

// ExampleRunSparseHeterDimArray shows and example creation, writing and reading of
// a sparse array using heterogeneus dimensions
func ExampleRunSparseHeterDimArray() {
	examples_lib.RunSparseHeterDimArray()

	// Output: Non-empty domain: [1.100000,1.400000], [1,4]
	// D1 Buffer: [1.1 1.2 1.3 1.4]
	// D2 Buffer: [1 2 3 4]
	// A Attribute Data: [1 2 3 4]
}
