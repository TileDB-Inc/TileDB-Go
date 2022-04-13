/**
 * @file   vacuum_test.go
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
 * When run, this program will create a simple 2D sparse array, write some data
 * to it, write again and read num of fragments. Then read from array,
 * consolidate and again read num of fragmens. Then vacuum and read number of
 * fragments.Finally will read from array to verify data read are the same as
 * in first read
 *
 */

package examples

import "github.com/TileDB-Inc/TileDB-Go/examples_lib"

// ExampleVacuumSparseArray shows ysage of array vacuum function
func ExampleVacuumSparseArray() {
	examples_lib.RunVacuumSparseArray()

	// Output: Estimated query size in bytes for attribute 'a': 12
	// Estimated query size in bytes for dimension 'd': 12
	// Cell (1) has data 1
	// Cell (2) has data 2
	// Cell (3) has data 3
	// Num of fragments after 2 writes before consolidate: 4
	// Num of fragments after consolidate: 4
	// Num of fragments after vacuum: 4
	// Estimated query size in bytes for attribute 'a': 12
	// Estimated query size in bytes for dimension 'd': 12
	// Cell (1) has data 1
	// Cell (2) has data 2
	// Cell (3) has data 3
}
