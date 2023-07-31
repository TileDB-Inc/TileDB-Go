//go:build experimental

// This file declares Go bindings for experimental features in TileDB.
// Experimental APIs to do not fall under the API compatibility guarantees and
// might change between TileDB versions

package tiledb

/*
#cgo LDFLAGS: -ltiledb
#cgo linux LDFLAGS: -ldl
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
		return 0, fmt.Errorf("Error getting relevant fragment num from query: %s", q.context.LastError())
	}

	return uint64(num), nil
}

// StatusDetails returns extended query status details.
func (q *Query) StatusDetails() (QueryStatusDetails, error) {
	var details QueryStatusDetails
	var cDetails C.tiledb_query_status_details_t
	if ret := C.tiledb_query_get_status_details(q.context.tiledbContext, q.tiledbQuery, &cDetails); ret != C.TILEDB_OK {
		return details, fmt.Errorf("Error getting query status details: %s", q.context.LastError())
	}
	details.IncompleteReason = QueryStatusDetailsReason(cDetails.incomplete_reason)
	return details, nil
}

// getDimensionLabelDataType Retrieve a dimension label Datatype from the schema using experimental APIs.
func (q *Query) getDimensionLabelDataType(labelName string) (Datatype, error) {
	schema, err := q.array.Schema()
	if err != nil {
		return 0, fmt.Errorf("Could not get schema for getDimensionLabelDatatype: %s", err)
	}

	dimLabel, err := schema.DimensionLabelFromName(labelName)
	if err != nil {
		return 0, fmt.Errorf("Could not get dimension label %s for getDimensionLabelDatatype: %s", labelName, err)
	}

	datatype, err := dimLabel.Type()
	if err != nil {
		return 0, fmt.Errorf("Could not get dimension label type for getDimensionLabelDatatype: %s", err)
	}

	return datatype, nil
}
