/**
 * @file   reading_timestamp_test.go
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

import "github.com/TileDB-Inc/TileDB-Go/examples_lib"

// ExampleTimestampArray shows timestamp correlation of written data and metadata
func ExampleTimestampArray() {
	examples_lib.RunTimestampArray()

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
