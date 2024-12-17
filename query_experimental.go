package tiledb

/*
#include <tiledb/tiledb.h>
#include <tiledb/tiledb_experimental.h>
#include <stdlib.h>
*/
import "C"

import (
	"fmt"
)

// QueryStatusDetails contains detailed information about the query status
type QueryStatusDetails struct {
	IncompleteReason QueryStatusDetailsReason
}

func (q *Query) RelevantFragmentNum() (uint64, error) {
	var num C.uint64_t
	if ret := C.tiledb_query_get_relevant_fragment_num(q.context.tiledbContext, q.tiledbQuery, &num); ret != C.TILEDB_OK {
		return 0, fmt.Errorf("error getting relevant fragment num from query: %w", q.context.LastError())
	}

	return uint64(num), nil
}

// StatusDetails returns extended query status details.
func (q *Query) StatusDetails() (QueryStatusDetails, error) {
	var details QueryStatusDetails
	var cDetails C.tiledb_query_status_details_t
	if ret := C.tiledb_query_get_status_details(q.context.tiledbContext, q.tiledbQuery, &cDetails); ret != C.TILEDB_OK {
		return details, fmt.Errorf("error getting query status details: %w", q.context.LastError())
	}
	details.IncompleteReason = QueryStatusDetailsReason(cDetails.incomplete_reason)
	return details, nil
}

// GetPlan returns a json encoding of the query plan for the query.
// Example:
//
//	{
//	    "TileDB Query Plan": {
//	        "Array.Type": "sparse",
//	        "Array.URI": "file:///tmp/TestHandleQueryPlanRequest732268097/001/t-testhandlequeryplanrequest-b757271e",
//	        "Query.Attributes": [
//	            "a1",
//	            "a2",
//	            "a3",
//	            "a4",
//	            "a5"
//	        ],
//	        "Query.Dimensions": [
//	            "dim1"
//	        ],
//	        "Query.Layout": "unordered",
//	        "Query.Strategy.Name": "UnorderedWriter",
//	        "VFS.Backend": "file"
//	    }
//	}
func (q *Query) GetPlan() (string, error) {
	var plan *C.tiledb_string_t

	ret := C.tiledb_query_get_plan(q.context.tiledbContext, q.tiledbQuery, &plan)
	if ret != C.TILEDB_OK {
		return "", fmt.Errorf("error getting query plan: %w", q.context.LastError())
	}
	defer C.tiledb_string_free(&plan)

	var sPlan *C.char
	var sPlanSize C.size_t
	ret = C.tiledb_string_view(plan, &sPlan, &sPlanSize)
	if ret != C.TILEDB_OK {
		return "", fmt.Errorf("error extracting query query: %w", q.context.LastError())
	}

	return C.GoStringN(sPlan, C.int(sPlanSize)), nil
}
