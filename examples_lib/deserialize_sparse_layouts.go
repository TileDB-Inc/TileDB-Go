package examples_lib

import (
	"fmt"
	"reflect"
)

// ToDo: Add proper test for deserialization
func RunDeserializeSparseLayouts() {
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
}
