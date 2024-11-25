package tiledb

import (
	"errors"
	"fmt"
	"reflect"
)

// DimensionType is a constraint for the types allowed for a TileDB dimension
type DimensionType interface {
	~string | ~float32 | ~float64 | ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~int | ~int8 | ~int16 | ~int32 | ~int64 | ~bool
}

// Range is an 1D range along a subarray dimension
type Range struct {
	start any // start of range, inclusive
	end   any // end of range, inclusive
}

// MakeRange returns a typed range [from, to]. It can be used with AddRange to add ranges to a dimension.
func MakeRange[T DimensionType](start, end T) Range {
	return Range{start: start, end: end}
}

// ExtractRange extracts the endpoints of the range.
// It returns []T{start, end, stride}. The stride is not supported by TileDB core yet,
// so it gets the zero value of T.
func ExtractRange[T DimensionType](r Range) ([]T, error) {
	// we compare reflect.Kind because they give more versatility. Reflect.Type is more strict
	// Consider:
	// type Tag string
	// type Label string
	// Label and Tag have different Type but same Kind. To interface with TileDB Core we use Kind
	// which is reflected to the core dimensions types.
	tKind := genericType[T]().Kind()
	rKind := reflect.ValueOf(r.start).Kind()
	if tKind != rKind {
		return nil, fmt.Errorf("cannot extract a range of %T to a slice of %v", r.start, genericType[T]())
	}

	res := make([]T, 3)
	res[0] = r.start.(T)
	res[1] = r.end.(T)
	// res[2] is stride

	return res, nil
}

// Endpoints returns the endpoint of the range. This is useful
// to print the range or serialize it, without infering the type first.
func (r Range) Endpoints() (start, end any) {
	return r.start, r.end
}

// assertCompatibility checks that the datatype of an array dimension are the same as the range's.
func (r Range) assertCompatibility(dimType Datatype, dimIsVar bool) error {
	dKind := dimType.ReflectKind()
	rKind := reflect.ValueOf(r.start).Kind()
	rIsVar := rKind == reflect.String

	if dimIsVar && dKind != reflect.Uint8 {
		return errors.New("only []byte var dimensions are supported")
	}
	if dimIsVar && !rIsVar {
		return errors.New("dimension is of variable size but range is not")
	}
	if !dimIsVar && rIsVar {
		return errors.New("range is of variable size but dimension is not")
	}
	if !dimIsVar && dKind != rKind {
		return fmt.Errorf("dimension and range types mismatch, range: %s dimension: %s", rKind, dKind)
	}

	return nil
}
