/**
 * @file   multi_attribute_test.go
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
 *   https://docs.tiledb.io/en/latest/tutorials/multi-attribute-arrays.html
 *
 * When run, this program will create a simple 2D dense array with two
 * attributes, write some data to it, and read a slice of the data back on
 * (i) both attributes, and (ii) subselecting on only one of the attributes.
 *
 */

package examples

import "github.com/TileDB-Inc/TileDB-Go/examples_lib"

func ExampleRunMultiAttributeArray() {
	examples_lib.RunMultiAttributeArray()

	// Output: Reading both attributes a1 and a2:
	// a1: b, a2: (2.1,2.2)
	// a1: c, a2: (3.1,3.2)
	// a1: d, a2: (4.1,4.2)
	// a1: f, a2: (6.1,6.2)
	// a1: g, a2: (7.1,7.2)
	// a1: h, a2: (8.1,8.2)
	//
	// Subselecting on attribute a1:
	// a1: b
	// a1: c
	// a1: d
	// a1: f
	// a1: g
	// a1: h
}
