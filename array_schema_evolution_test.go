//go:build experimental
// +build experimental

package tiledb

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

const array_schema_evolution_name = "array_schema_evolution"

func TestArraySchemaEvolution(t *testing.T) {
	config, err := NewConfig()
	assert.Nil(t, err)
	assert.NotNil(t, config)

	// Test context with config
	context, err := NewContext(config)
	assert.Nil(t, err)
	assert.NotNil(t, context)

	dimension, err := NewDimension(context, "test", TILEDB_INT32, []int32{1, 10}, int32(5))
	assert.Nil(t, err)
	assert.NotNil(t, dimension)

	domain, err := NewDomain(context)
	assert.Nil(t, err)
	assert.NotNil(t, domain)

	// Add dimension to domain
	err = domain.AddDimensions(dimension)
	assert.Nil(t, err)

	arraySchema, err := NewArraySchema(context, TILEDB_DENSE)
	assert.Nil(t, err)
	assert.NotNil(t, arraySchema)

	a1, err := NewAttribute(context, "a1", TILEDB_INT32)
	assert.Nil(t, err)
	assert.NotNil(t, a1)

	a2, err := NewAttribute(context, "a2", TILEDB_INT32)
	assert.Nil(t, err)
	assert.NotNil(t, a2)

	err = arraySchema.AddAttributes(a1, a2)
	assert.Nil(t, err)

	err = arraySchema.SetCapacity(100)
	assert.Nil(t, err)

	err = arraySchema.SetDomain(domain)
	assert.Nil(t, err)

	err = arraySchema.Check()
	assert.Nil(t, err)

	array, err := NewArray(context, array_schema_evolution_name)
	assert.Nil(t, err)
	assert.NotNil(t, array)

	defer os.RemoveAll(array_schema_evolution_name)

	err = array.Create(arraySchema)
	assert.Nil(t, err)

	err = array.Close()
	assert.Nil(t, err)

	arraySchemaEvolution, err := NewArraySchemaEvolution(context)
	assert.Nil(t, err)
	defer arraySchemaEvolution.Free()

	a3, err := NewAttribute(context, "a3", TILEDB_INT32)
	assert.Nil(t, err)
	assert.NotNil(t, a2)

	err = arraySchemaEvolution.AddAttribute(a3)
	assert.Nil(t, err)

	// Will fail when try to add an attribute which already has the name
	err = arraySchemaEvolution.AddAttribute(a3)
	assert.NotNil(t, err)

	// Remove atrribute a1
	err = arraySchemaEvolution.DropAttribute("a1")
	assert.Nil(t, err)

	err = arraySchemaEvolution.Evolve(array_schema_evolution_name)
	assert.Nil(t, err)

	// Validate schema evolution changes
	ctx, err := NewContext(nil)
	assert.Nil(t, err)
	defer ctx.Free()

	// Prepare the array for reading
	arr, err := NewArray(ctx, array_schema_evolution_name)
	assert.Nil(t, err)
	defer array.Free()

	err = arr.Open(TILEDB_READ)
	assert.Nil(t, err)

	// Need to get the evolved schema
	arrAchema, err := arr.Schema()
	assert.Nil(t, err)
	assert.NotNil(t, arrAchema)

	hasAttr, err := arrAchema.HasAttribute("a1")
	assert.Nil(t, err)
	assert.Equal(t, false, hasAttr)

	hasAttr, err = arrAchema.HasAttribute("a2")
	assert.Nil(t, err)
	assert.Equal(t, true, hasAttr)

	hasAttr, err = arrAchema.HasAttribute("a3")
	assert.Nil(t, err)
	assert.Equal(t, true, hasAttr)

	attrNum, err := arrAchema.AttributeNum()
	assert.Nil(t, err)
	assert.Equal(t, uint(2), attrNum)

	attrFromIndex, err := arrAchema.AttributeFromIndex(0)
	assert.Nil(t, err)
	assert.NotNil(t, attrFromIndex)

	attrName, err := attrFromIndex.Name()
	assert.Nil(t, err)
	assert.Equal(t, "a2", attrName)

	attrFromName, err := arrAchema.AttributeFromName("a3")
	assert.Nil(t, err)
	assert.NotNil(t, attrFromName)

	attrName2, err := attrFromName.Name()
	assert.Nil(t, err)
	assert.Equal(t, "a3", attrName2)

	err = arr.Close()
	assert.Nil(t, err)
}
