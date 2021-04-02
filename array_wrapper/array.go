package array_wrapper

import (
	tiledb "github.com/TileDB-Inc/TileDB-Go"
)

type DimensionDetail struct {
	Domain interface{}
	Extent interface{}
}

type AttributeDetail struct {
	Datatype tiledb.Datatype
}

type ArrayWrapper struct {
	name string
}

func createArray(arrayName string, arrayType tiledb.ArrayType,
	config *tiledb.Config,
	cellOrder tiledb.Layout,
	tileOrder tiledb.Layout,
	dimMap map[string]DimensionDetail,
	attrMap map[string]AttributeDetail) error {

	ctx, err := tiledb.NewContext(nil)
	if err != nil {
		return err
	}
	defer ctx.Free()

	domain, err := tiledb.NewDomain(ctx)
	if err != nil {
		return err
	}
	defer domain.Free()

	for dimName, dimDetail := range dimMap {
		dim, err := tiledb.NewDimension(ctx, dimName, dimDetail.Domain, dimDetail.Extent)
		if err != nil {
			return err
		}
		err = domain.AddDimensions(dim)
		if err != nil {
			return err
		}
		defer dim.Free()
	}

	// The array will be dense.
	schema, err := tiledb.NewArraySchema(ctx, arrayType)
	if err != nil {
		return err
	}
	defer schema.Free()

	err = schema.SetDomain(domain)
	if err != nil {
		return err
	}
	err = schema.SetCellOrder(cellOrder)
	if err != nil {
		return err
	}

	err = schema.SetTileOrder(tileOrder)
	if err != nil {
		return err
	}

	for attrName, attrDetail := range attrMap {
		attr, err := tiledb.NewAttribute(ctx, attrName, attrDetail.Datatype)
		if err != nil {
			return err
		}
		err = schema.AddAttributes(attr)
		if err != nil {
			return err
		}
		defer attr.Free()
	}

	// Create the (empty) array on disk.
	array, err := tiledb.NewArray(ctx, arrayName)
	if err != nil {
		return err
	}
	defer array.Free()

	err = array.Create(schema)
	if err != nil {
		return err
	}

	return nil
}

func NewDenseArray(arrayName string, cellOrder tiledb.Layout,
	tileOrder tiledb.Layout,
	dimMap map[string]DimensionDetail,
	attrMap map[string]AttributeDetail) (*ArrayWrapper, error) {
	arrayWrapper := ArrayWrapper{name: arrayName}

	err := createArray(arrayName, tiledb.TILEDB_DENSE, nil, cellOrder,
		tileOrder, dimMap, attrMap)
	if err != nil {
		return nil, err
	}

	return &arrayWrapper, nil
}

func NewSparseArray(arrayName string, cellOrder tiledb.Layout,
	tileOrder tiledb.Layout,
	dimMap map[string]DimensionDetail,
	attrMap map[string]AttributeDetail) (*ArrayWrapper, error) {
	arrayWrapper := ArrayWrapper{name: arrayName}

	err := createArray(arrayName, tiledb.TILEDB_SPARSE, nil, cellOrder,
		tileOrder, dimMap, attrMap)
	if err != nil {
		return nil, err
	}

	return &arrayWrapper, nil
}
