package tiledb

/*
#cgo LDFLAGS: -ltiledb
#cgo linux LDFLAGS: -ldl
#include <tiledb/tiledb.h>
#include <stdlib.h>
*/
import "C"

import (
	"fmt"
	"reflect"
	"runtime"
	"unsafe"

	"github.com/TileDB-Inc/TileDB-Go/bytesizes"
)

// QueryCondition defines a condition used for a query.
type QueryCondition struct {
	context *Context
	cond    *C.tiledb_query_condition_t
}

// NewQueryCondition allocates and initializes a new query condition
func NewQueryCondition(tdbCtx *Context, attributeName string, op QueryConditionOp, value interface{}) (*QueryCondition, error) {
	qc := QueryCondition{context: tdbCtx}
	if ret := C.tiledb_query_condition_alloc(qc.context.tiledbContext, &qc.cond); ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error allocating tiledb query condition: %s", qc.context.LastError())
	}

	// Set finalizer for free C pointer on gc
	runtime.SetFinalizer(&qc, func(qc *QueryCondition) {
		qc.Free()
	})

	if err := qc.initQueryCondition(attributeName, value, op); err != nil {
		return nil, err
	}

	return &qc, nil
}

// NewQueryConditionCombination combines two query conditions to create a new query condition. The underlying conditions
// are unchanged
func NewQueryConditionCombination(tdbCtx *Context, left *QueryCondition, op QueryConditionCombinationOp, right *QueryCondition) (*QueryCondition, error) {
	qc := QueryCondition{context: tdbCtx}
	if ret := C.tiledb_query_condition_combine(qc.context.tiledbContext, left.cond, right.cond, C.tiledb_query_condition_combination_op_t(op), &qc.cond); ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error allocating tiledb query condition: %s", qc.context.LastError())
	}

	// Set finalizer for free C pointer on gc
	runtime.SetFinalizer(&qc, func(qc *QueryCondition) {
		qc.Free()
	})

	return &qc, nil
}

// Free tiledb_query_condition_t that was allocated on heap in c
func (qc *QueryCondition) Free() {
	if qc.cond != nil {
		C.tiledb_query_condition_free(&qc.cond)
	}
}

func (qc *QueryCondition) initQueryCondition(attributeName string, value interface{}, op QueryConditionOp) error {
	cname := C.CString(attributeName)
	defer C.free(unsafe.Pointer(cname))

	size, err := getSize(value)
	if err != nil {
		return fmt.Errorf("Error initing tiledb query condition for attribute: %s error: %s", attributeName, err)
	}

	switch v := value.(type) {
	case string:
		valueSize := C.uint64_t(len(v))
		cTmpValue := C.CString(v)
		defer C.free(unsafe.Pointer(cTmpValue))
		if ret := C.tiledb_query_condition_init(qc.context.tiledbContext, qc.cond, cname, unsafe.Pointer(cTmpValue), valueSize, C.tiledb_query_condition_op_t(op)); ret != C.TILEDB_OK {
			return fmt.Errorf("Error initing tiledb string query condition: %s", qc.context.LastError())
		}
	case int:
		if ret := C.tiledb_query_condition_init(qc.context.tiledbContext, qc.cond, cname, unsafe.Pointer(&v), C.uint64_t(size), C.tiledb_query_condition_op_t(op)); ret != C.TILEDB_OK {
			return fmt.Errorf("Error initing tiledb numeric query condition: %s", qc.context.LastError())
		}
	case int8:
		if ret := C.tiledb_query_condition_init(qc.context.tiledbContext, qc.cond, cname, unsafe.Pointer(&v), C.uint64_t(size), C.tiledb_query_condition_op_t(op)); ret != C.TILEDB_OK {
			return fmt.Errorf("Error initing tiledb numeric query condition: %s", qc.context.LastError())
		}
	case int32:
		if ret := C.tiledb_query_condition_init(qc.context.tiledbContext, qc.cond, cname, unsafe.Pointer(&v), C.uint64_t(size), C.tiledb_query_condition_op_t(op)); ret != C.TILEDB_OK {
			return fmt.Errorf("Error initing tiledb numeric query condition: %s", qc.context.LastError())
		}
	case int64:
		if ret := C.tiledb_query_condition_init(qc.context.tiledbContext, qc.cond, cname, unsafe.Pointer(&v), C.uint64_t(size), C.tiledb_query_condition_op_t(op)); ret != C.TILEDB_OK {
			return fmt.Errorf("Error initing tiledb numeric query condition: %s", qc.context.LastError())
		}
	case uint:
		if ret := C.tiledb_query_condition_init(qc.context.tiledbContext, qc.cond, cname, unsafe.Pointer(&v), C.uint64_t(size), C.tiledb_query_condition_op_t(op)); ret != C.TILEDB_OK {
			return fmt.Errorf("Error initing tiledb numeric query condition: %s", qc.context.LastError())
		}
	case uint8:
		if ret := C.tiledb_query_condition_init(qc.context.tiledbContext, qc.cond, cname, unsafe.Pointer(&v), C.uint64_t(size), C.tiledb_query_condition_op_t(op)); ret != C.TILEDB_OK {
			return fmt.Errorf("Error initing tiledb numeric query condition: %s", qc.context.LastError())
		}
	case uint16:
		if ret := C.tiledb_query_condition_init(qc.context.tiledbContext, qc.cond, cname, unsafe.Pointer(&v), C.uint64_t(size), C.tiledb_query_condition_op_t(op)); ret != C.TILEDB_OK {
			return fmt.Errorf("Error initing tiledb numeric query condition: %s", qc.context.LastError())
		}
	case uint32:
		if ret := C.tiledb_query_condition_init(qc.context.tiledbContext, qc.cond, cname, unsafe.Pointer(&v), C.uint64_t(size), C.tiledb_query_condition_op_t(op)); ret != C.TILEDB_OK {
			return fmt.Errorf("Error initing tiledb numeric query condition: %s", qc.context.LastError())
		}
	case uint64:
		if ret := C.tiledb_query_condition_init(qc.context.tiledbContext, qc.cond, cname, unsafe.Pointer(&v), C.uint64_t(size), C.tiledb_query_condition_op_t(op)); ret != C.TILEDB_OK {
			return fmt.Errorf("Error initing tiledb numeric query condition: %s", qc.context.LastError())
		}
	case float32:
		if ret := C.tiledb_query_condition_init(qc.context.tiledbContext, qc.cond, cname, unsafe.Pointer(&v), C.uint64_t(size), C.tiledb_query_condition_op_t(op)); ret != C.TILEDB_OK {
			return fmt.Errorf("Error initing tiledb numeric query condition: %s", qc.context.LastError())
		}
	case float64:
		if ret := C.tiledb_query_condition_init(qc.context.tiledbContext, qc.cond, cname, unsafe.Pointer(&v), C.uint64_t(size), C.tiledb_query_condition_op_t(op)); ret != C.TILEDB_OK {
			return fmt.Errorf("Error initing tiledb numeric query condition: %s", qc.context.LastError())
		}
	case []int:
		if ret := C.tiledb_query_condition_init(qc.context.tiledbContext, qc.cond, cname, unsafe.Pointer(&v[0]), C.uint64_t(size), C.tiledb_query_condition_op_t(op)); ret != C.TILEDB_OK {
			return fmt.Errorf("Error initing tiledb numeric slice query condition: %s", qc.context.LastError())
		}
	case []int8:
		if ret := C.tiledb_query_condition_init(qc.context.tiledbContext, qc.cond, cname, unsafe.Pointer(&v[0]), C.uint64_t(size), C.tiledb_query_condition_op_t(op)); ret != C.TILEDB_OK {
			return fmt.Errorf("Error initing tiledb numeric slice query condition: %s", qc.context.LastError())
		}
	case []int32:
		if ret := C.tiledb_query_condition_init(qc.context.tiledbContext, qc.cond, cname, unsafe.Pointer(&v[0]), C.uint64_t(size), C.tiledb_query_condition_op_t(op)); ret != C.TILEDB_OK {
			return fmt.Errorf("Error initing tiledb numeric slice query condition: %s", qc.context.LastError())
		}
	case []int64:
		if ret := C.tiledb_query_condition_init(qc.context.tiledbContext, qc.cond, cname, unsafe.Pointer(&v[0]), C.uint64_t(size), C.tiledb_query_condition_op_t(op)); ret != C.TILEDB_OK {
			return fmt.Errorf("Error initing tiledb numeric slice query condition: %s", qc.context.LastError())
		}
	case []uint:
		if ret := C.tiledb_query_condition_init(qc.context.tiledbContext, qc.cond, cname, unsafe.Pointer(&v[0]), C.uint64_t(size), C.tiledb_query_condition_op_t(op)); ret != C.TILEDB_OK {
			return fmt.Errorf("Error initing tiledb numeric slice query condition: %s", qc.context.LastError())
		}
	case []uint8:
		if ret := C.tiledb_query_condition_init(qc.context.tiledbContext, qc.cond, cname, unsafe.Pointer(&v[0]), C.uint64_t(size), C.tiledb_query_condition_op_t(op)); ret != C.TILEDB_OK {
			return fmt.Errorf("Error initing tiledb numeric slice query condition: %s", qc.context.LastError())
		}
	case []uint16:
		if ret := C.tiledb_query_condition_init(qc.context.tiledbContext, qc.cond, cname, unsafe.Pointer(&v[0]), C.uint64_t(size), C.tiledb_query_condition_op_t(op)); ret != C.TILEDB_OK {
			return fmt.Errorf("Error initing tiledb numeric slice query condition: %s", qc.context.LastError())
		}
	case []uint32:
		if ret := C.tiledb_query_condition_init(qc.context.tiledbContext, qc.cond, cname, unsafe.Pointer(&v[0]), C.uint64_t(size), C.tiledb_query_condition_op_t(op)); ret != C.TILEDB_OK {
			return fmt.Errorf("Error initing tiledb numeric slice query condition: %s", qc.context.LastError())
		}
	case []uint64:
		if ret := C.tiledb_query_condition_init(qc.context.tiledbContext, qc.cond, cname, unsafe.Pointer(&v[0]), C.uint64_t(size), C.tiledb_query_condition_op_t(op)); ret != C.TILEDB_OK {
			return fmt.Errorf("Error initing tiledb numeric slice query condition: %s", qc.context.LastError())
		}
	case []float32:
		if ret := C.tiledb_query_condition_init(qc.context.tiledbContext, qc.cond, cname, unsafe.Pointer(&v[0]), C.uint64_t(size), C.tiledb_query_condition_op_t(op)); ret != C.TILEDB_OK {
			return fmt.Errorf("Error initing tiledb numeric slice query condition: %s", qc.context.LastError())
		}
	case []float64:
		if ret := C.tiledb_query_condition_init(qc.context.tiledbContext, qc.cond, cname, unsafe.Pointer(&v[0]), C.uint64_t(size), C.tiledb_query_condition_op_t(op)); ret != C.TILEDB_OK {
			return fmt.Errorf("Error initing tiledb numeric slice query condition: %s", qc.context.LastError())
		}
	default:
		return fmt.Errorf("Unhandled query condition value type: %s", reflect.TypeOf(v))
	}
	return nil
}

func getSize(v interface{}) (uint64, error) {
	if reflect.TypeOf(v).Kind() == reflect.Slice || reflect.TypeOf(v).Kind() == reflect.Array {
		return sizeOfSlice(v)
	}

	size, ok := bytesizes.Kind[reflect.TypeOf(v).Kind()]
	if !ok {
		return 0, fmt.Errorf("Error determining size of value kind: %v", reflect.TypeOf(v).Kind())
	}
	return size, nil
}

func sizeOfSlice(v interface{}) (uint64, error) {
	elemSize, ok := bytesizes.Kind[reflect.TypeOf(v).Elem().Kind()]
	if !ok {
		return 0, fmt.Errorf("Error determining type of value kind: %v", reflect.TypeOf(v).Elem().Kind())
	}

	return elemSize * uint64(reflect.ValueOf(v).Len()), nil
}
