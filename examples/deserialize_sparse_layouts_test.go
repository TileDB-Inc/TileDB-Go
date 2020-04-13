/**
 * @file   reading_sparse_layouts_test.go
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018 TileDB, Inc.
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
 */

package examples

import (
	"fmt"
	"reflect"
)

// ToDo: Add proper test for deserialization
func ExampleDeserializeSparseLayouts() {
	var ndims uint = 2
	tmpDomainArray := []uint32{1, 2, 1, 4}
	tmpDomainArrayType := reflect.TypeOf(tmpDomainArray).Elem().Kind()
	tmpDomainInterface := interface{}(tmpDomainArray)
	for i := uint(0); i < ndims; i++ {

		switch tmpDomainArrayType {
		case reflect.Uint32:
			tmpSubArray := tmpDomainInterface.([]uint32)
			tmpDimension := make([]interface{}, 2)
			tmpDimension[0] = tmpSubArray[(2 * i)]
			tmpDimension[1] = tmpSubArray[(2*i)+1]
			fmt.Printf("%v\n", tmpDimension)
		default:
			fmt.Printf("unhandled subarray tmpDomainArrayType: %s\n", tmpDomainArrayType.String())
		}

	}

	// Output: [1 2]
	// [1 4]
}
