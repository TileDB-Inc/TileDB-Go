/**
 * @file   quickstart_dense_large_test.go
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
 * When run, this program will create a simple 2D dense array, write some data
 * to it, and read a slice of the data back in the layout of the user's choice
 * (passed as an argument to the program: "row", "col", or "global").
 *
 */

package examples

import (
	"github.com/TileDB-Inc/TileDB-Go/examples_lib"
)

// ExampleDenseLargeArray shows and example creation, writing and reading of a dense
// array
func ExampleDenseLargeArray() {
	examples_lib.RunDenseLargeArray()

	// Output: Read: 1
	// [30008 30009 30010 30011 30012 30013 30014 30015 30016 30017 30018 30019 30020 30021 30022 30023 30024 30025]
	// Read: 2
	// [30008 30009 30010 30011 30012 30013 30014 30015 30016 30017 30018 30019 30020 30021 30022 30023 30024 30025]
	// Read: 3
	// [30008 30009 30010 30011 30012 30013 30014 30015 30016 30017 30018 30019 30020 30021 30022 30023 30024 30025]
	// Read: 4
	// [30008 30009 30010 30011 30012 30013 30014 30015 30016 30017 30018 30019 30020 30021 30022 30023 30024 30025]
	// Read: 5
	// [30008 30009 30010 30011 30012 30013 30014 30015 30016 30017 30018 30019 30020 30021 30022 30023 30024 30025]
	// Read: 6
	// [30008 30009 30010 30011 30012 30013 30014 30015 30016 30017 30018 30019 30020 30021 30022 30023 30024 30025]
	// Read: 7
	// [30008 30009 30010 30011 30012 30013 30014 30015 30016 30017 30018 30019 30020 30021 30022 30023 30024 30025]
	// Read: 8
	// [30008 30009 30010 30011 30012 30013 30014 30015 30016 30017 30018 30019 30020 30021 30022 30023 30024 30025]
	// Read: 9
	// [30008 30009 30010 30011 30012 30013 30014 30015 30016 30017 30018 30019 30020 30021 30022 30023 30024 30025]
	// Read: 10
	// [30008 30009 30010 30011 30012 30013 30014 30015 30016 30017 30018 30019 30020 30021 30022 30023 30024 30025]
}
