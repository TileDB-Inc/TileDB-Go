package tiledb

/*
#include <tiledb/tiledb.h>
#include <stdlib.h>
*/
import "C"

import (
	"fmt"
	"runtime"
	"unsafe"
)

type queryConditionHandle struct{ *capiHandle }

func freeCapiQueryCondition(c unsafe.Pointer) {
	C.tiledb_query_condition_free((**C.tiledb_query_condition_t)(unsafe.Pointer(&c)))
}

func newQueryConditionHandle(ptr *C.tiledb_query_condition_t) queryConditionHandle {
	return queryConditionHandle{newCapiHandle(unsafe.Pointer(ptr), freeCapiQueryCondition)}
}

func (x queryConditionHandle) Get() *C.tiledb_query_condition_t {
	return (*C.tiledb_query_condition_t)(x.capiHandle.Get())
}

// QueryCondition defines a condition used for a query.
type QueryCondition struct {
	context *Context
	cond    queryConditionHandle
}

func newQueryConditionFromHandle(tdbCtx *Context, handle queryConditionHandle) *QueryCondition {
	return &QueryCondition{context: tdbCtx, cond: handle}
}

// NewQueryCondition allocates and initializes a new query condition.
func NewQueryCondition(tdbCtx *Context, attributeName string, op QueryConditionOp, value interface{}) (*QueryCondition, error) {
	var qcPtr *C.tiledb_query_condition_t
	if ret := C.tiledb_query_condition_alloc(tdbCtx.tiledbContext.Get(), &qcPtr); ret != C.TILEDB_OK {
		return nil, fmt.Errorf("error allocating tiledb query condition: %w", tdbCtx.LastError())
	}
	runtime.KeepAlive(tdbCtx)

	qc := newQueryConditionFromHandle(tdbCtx, newQueryConditionHandle(qcPtr))
	if err := qc.init(attributeName, value, op); err != nil {
		return nil, err
	}

	return qc, nil
}

// NewQueryConditionCombination combines two query conditions to create a new query condition. The underlying conditions
// are unchanged.
func NewQueryConditionCombination(tdbCtx *Context, left *QueryCondition, op QueryConditionCombinationOp, right *QueryCondition) (*QueryCondition, error) {
	var qcPtr *C.tiledb_query_condition_t
	if ret := C.tiledb_query_condition_combine(tdbCtx.tiledbContext.Get(), left.cond.Get(), right.cond.Get(), C.tiledb_query_condition_combination_op_t(op), &qcPtr); ret != C.TILEDB_OK {
		return nil, fmt.Errorf("error allocating tiledb query condition: %w", tdbCtx.LastError())
	}
	runtime.KeepAlive(tdbCtx)
	runtime.KeepAlive(left)
	runtime.KeepAlive(right)

	return newQueryConditionFromHandle(tdbCtx, newQueryConditionHandle(qcPtr)), nil
}

// NewQueryConditionNegated returns the negation of the query condition. The initial condition
// is unchanged.
func NewQueryConditionNegated(tdbCtx *Context, qc *QueryCondition) (*QueryCondition, error) {
	var nqcPtr *C.tiledb_query_condition_t
	if ret := C.tiledb_query_condition_negate(qc.context.tiledbContext.Get(), qc.cond.Get(), &nqcPtr); ret != C.TILEDB_OK {
		return nil, fmt.Errorf("error allocating tiledb query condition: %w", tdbCtx.LastError())
	}
	runtime.KeepAlive(tdbCtx)
	runtime.KeepAlive(qc)

	return newQueryConditionFromHandle(tdbCtx, newQueryConditionHandle(nqcPtr)), nil
}

// Free releases the internal TileDB core data that was allocated on the C heap.
// It is automatically called when this object is garbage collected, but can be
// called earlier to manually release memory if needed. Free is idempotent and
// can safely be called many times on the same object; if it has already
// been freed, it will not be freed again.
func (qc *QueryCondition) Free() {
	qc.cond.Free()
}

// Context exposes the internal TileDB context used to initialize the query condition
func (qc *QueryCondition) Context() *Context {
	return qc.context
}

func (qc *QueryCondition) init(attributeName string, value interface{}, op QueryConditionOp) error {
	switch value := value.(type) {
	case int:
		return qcInitScalar(qc, attributeName, value, op)
	case []int:
		return qcInitSlice(qc, attributeName, value, op)
	case int8:
		return qcInitScalar(qc, attributeName, value, op)
	case []int8:
		return qcInitSlice(qc, attributeName, value, op)
	case int16:
		return qcInitScalar(qc, attributeName, value, op)
	case []int16:
		return qcInitSlice(qc, attributeName, value, op)
	case int32:
		return qcInitScalar(qc, attributeName, value, op)
	case []int32:
		return qcInitSlice(qc, attributeName, value, op)
	case int64:
		return qcInitScalar(qc, attributeName, value, op)
	case []int64:
		return qcInitSlice(qc, attributeName, value, op)
	case uint:
		return qcInitScalar(qc, attributeName, value, op)
	case []uint:
		return qcInitSlice(qc, attributeName, value, op)
	case uint8:
		return qcInitScalar(qc, attributeName, value, op)
	case []uint8:
		return qcInitSlice(qc, attributeName, value, op)
	case uint16:
		return qcInitScalar(qc, attributeName, value, op)
	case []uint16:
		return qcInitSlice(qc, attributeName, value, op)
	case uint32:
		return qcInitScalar(qc, attributeName, value, op)
	case []uint32:
		return qcInitSlice(qc, attributeName, value, op)
	case uint64:
		return qcInitScalar(qc, attributeName, value, op)
	case []uint64:
		return qcInitSlice(qc, attributeName, value, op)
	case float32:
		return qcInitScalar(qc, attributeName, value, op)
	case []float32:
		return qcInitSlice(qc, attributeName, value, op)
	case float64:
		return qcInitScalar(qc, attributeName, value, op)
	case []float64:
		return qcInitSlice(qc, attributeName, value, op)
	case bool:
		return qcInitScalar(qc, attributeName, value, op)
	case []bool:
		return qcInitSlice(qc, attributeName, value, op)
	case string:
		valuePtr := unsafe.Pointer(C.CString(value))
		defer C.free(valuePtr)
		return qcInitInternal(qc, attributeName, valuePtr, uint64(len(value)), op)
	}
	return fmt.Errorf("cannot create query condition for type %T", value)
}

func qcInitScalar[T scalarType](qc *QueryCondition, attributeName string, value T, op QueryConditionOp) error {
	return qcInitInternal(qc, attributeName, unsafe.Pointer(&value), uint64(unsafe.Sizeof(value)), op)
}

func qcInitSlice[T scalarType](qc *QueryCondition, attributeName string, value []T, op QueryConditionOp) error {
	var t T
	size := uint64(unsafe.Sizeof(t)) * uint64(len(value))
	return qcInitInternal(qc, attributeName, slicePtr(value), size, op)
}

func qcInitInternal(qc *QueryCondition, attributeName string, valuePtr unsafe.Pointer, valueSize uint64, op QueryConditionOp) error {
	cname := C.CString(attributeName)
	defer C.free(unsafe.Pointer(cname))
	ret := C.tiledb_query_condition_init(
		qc.context.tiledbContext.Get(),
		qc.cond.Get(),
		cname,
		valuePtr,
		C.uint64_t(valueSize),
		C.tiledb_query_condition_op_t(op),
	)
	runtime.KeepAlive(qc)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("could not init %q query condition: %w", attributeName, qc.context.LastError())
	}
	return nil
}
