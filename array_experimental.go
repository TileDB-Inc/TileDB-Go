package tiledb

/*
#include <tiledb/tiledb.h>
#include <tiledb/tiledb_experimental.h>
#include <stdlib.h>
*/
import "C"
import (
	"errors"
	"fmt"
	"runtime"
	"unsafe"
)

type consolidationPlanHandle struct{ *capiHandle }

func freeCapiConsolidationPlan(c unsafe.Pointer) {
	C.tiledb_consolidation_plan_free((**C.tiledb_consolidation_plan_t)(unsafe.Pointer(&c)))
}

func newConsolidationPlanHandle(ptr *C.tiledb_consolidation_plan_t) consolidationPlanHandle {
	return consolidationPlanHandle{newCapiHandle(unsafe.Pointer(ptr), freeCapiConsolidationPlan)}
}

func (x consolidationPlanHandle) Get() *C.tiledb_consolidation_plan_t {
	return (*C.tiledb_consolidation_plan_t)(x.capiHandle.Get())
}

// ConsolidationPlan is a consolidation plan for array
type ConsolidationPlan struct {
	tiledbConsolidationPlan consolidationPlanHandle
	context                 *Context
}

func newConsolidationPlanFromHandle(context *Context, handle consolidationPlanHandle) *ConsolidationPlan {
	return &ConsolidationPlan{tiledbConsolidationPlan: handle, context: context}
}

// GetConsolidationPlan creates a consolidation plan for the already opened array.
// The plan and the array will share the same tiledb context
func GetConsolidationPlan(arr *Array, fragmentSize uint64) (*ConsolidationPlan, error) {
	var consolidationPlanPtr *C.tiledb_consolidation_plan_t
	ret := C.tiledb_consolidation_plan_create_with_mbr(arr.context.tiledbContext, arr.tiledbArray.Get(), C.uint64_t(fragmentSize), &consolidationPlanPtr)
	runtime.KeepAlive(arr)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("error getting consolidation plan for array: %w", arr.context.LastError())
	}

	return newConsolidationPlanFromHandle(arr.context, newConsolidationPlanHandle(consolidationPlanPtr)), nil
}

// Free releases the internal TileDB core data that was allocated on the C heap.
// It is automatically called when this object is garbage collected, but can be
// called earlier to manually release memory if needed. Free is idempotent and
// can safely be called many times on the same object; if it has already
// been freed, it will not be freed again.
func (cp *ConsolidationPlan) Free() {
	cp.tiledbConsolidationPlan.Free()
}

// NumNodes returns the number of nodes for the plan
func (cp *ConsolidationPlan) NumNodes() (uint64, error) {
	var numNodes C.uint64_t

	ret := C.tiledb_consolidation_plan_get_num_nodes(cp.context.tiledbContext, cp.tiledbConsolidationPlan.Get(), &numNodes)
	runtime.KeepAlive(cp)
	if ret != C.TILEDB_OK {
		return 0, fmt.Errorf("error getting consolidation plan num nodes: %w", cp.context.LastError())
	}

	return uint64(numNodes), nil
}

// NumFragments returns the numner of fragments of the node
func (cp *ConsolidationPlan) NumFragments(nodeIndex uint64) (uint64, error) {
	var numFragments C.uint64_t

	ret := C.tiledb_consolidation_plan_get_num_fragments(cp.context.tiledbContext, cp.tiledbConsolidationPlan.Get(), C.uint64_t(nodeIndex), &numFragments)
	runtime.KeepAlive(cp)
	if ret != C.TILEDB_OK {
		return 0, fmt.Errorf("error getting consolidation plan num fragments: %w", cp.context.LastError())
	}

	return uint64(numFragments), nil
}

// FragmentURI returns the uri of the fragment of the node
func (cp *ConsolidationPlan) FragmentURI(nodeIndex, fragmentIndex uint64) (string, error) {
	var curi *C.char

	ret := C.tiledb_consolidation_plan_get_fragment_uri(cp.context.tiledbContext, cp.tiledbConsolidationPlan.Get(), C.uint64_t(nodeIndex), C.uint64_t(fragmentIndex), &curi)
	runtime.KeepAlive(cp)
	if ret != C.TILEDB_OK {
		return "", fmt.Errorf("error getting consolidation plan fragment uri for node %d and fragment %d: %w", nodeIndex, fragmentIndex, cp.context.LastError())
	}

	return C.GoString(curi), nil
}

// DumpJSON returns a json serialization of the plan
func (cp *ConsolidationPlan) DumpJSON() (string, error) {
	var cjson *C.char
	ret := C.tiledb_consolidation_plan_dump_json_str(cp.context.tiledbContext, cp.tiledbConsolidationPlan.Get(), &cjson)
	runtime.KeepAlive(cp)
	if ret != C.TILEDB_OK {
		return "", fmt.Errorf("error getting consolidation plan json dump: %w", cp.context.LastError())
	}

	json := C.GoString(cjson)

	ret = C.tiledb_consolidation_plan_free_json_str(&cjson)
	if ret != C.TILEDB_OK {
		return "", fmt.Errorf("error getting consolidation plan json dump: %w", cp.context.LastError())
	}

	return json, nil
}

// ConsolidateFragments consolidates an explicit list of fragments in an array into a single fragment.
// You must first finalize all queries to the array before consolidation can
// begin (as consolidation temporarily acquires an exclusive lock on the array).
func (a *Array) ConsolidateFragments(config *Config, fragmentList []string) error {
	if config == nil {
		return errors.New("config must not be nil for Consolidate")
	}

	curi := C.CString(a.uri)
	defer C.free(unsafe.Pointer(curi))

	list, freeMemory := cStringArray(fragmentList)
	defer freeMemory()

	ret := C.tiledb_array_consolidate_fragments(a.context.tiledbContext, curi, (**C.char)(slicePtr(list)), C.uint64_t(len(list)), config.tiledbConfig.Get())
	runtime.KeepAlive(a)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("error consolidating tiledb array fragment list: %w", a.context.LastError())
	}

	runtime.KeepAlive(config)
	return nil
}
