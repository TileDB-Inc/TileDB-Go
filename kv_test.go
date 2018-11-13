package tiledb

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// ExampleKV example of creating and using a Key-Value store
func ExampleKV() {

	// Create Context with default configuration
	context, err := NewContext(nil)

	if err != nil {
		// handle error
		return
	}

	// Create a KV schema
	kvSchema, err := NewKVSchema(context)
	if err != nil {
		// handle error
		return
	}

	// Add a attribute to the key value store
	a1, err := NewAttribute(context, "a1", TILEDB_INT8)
	if err != nil {
		// handle error
		return
	}

	err = kvSchema.AddAttributes(a1)
	if err != nil {
		// handle error
		return
	}

	// create temp KV folder
	tmpKVPath := os.TempDir() + string(os.PathSeparator) + "tiledb_test_kv"
	defer os.RemoveAll(tmpKVPath)
	if _, err = os.Stat(tmpKVPath); err == nil {
		os.RemoveAll(tmpKVPath)
	}

	// Create new KV Struct
	kv, err := NewKV(context, tmpKVPath)
	if err != nil {
		// handle error
		return
	}

	// Create new KV
	err = kv.Create(kvSchema)
	if err != nil {
		// handle error
		return
	}

	// Create new KV
	err = kv.Open(TILEDB_WRITE)
	if err != nil {
		// handle error
		return
	}

	// Create a new KVItem to add
	kvItem, err := NewKVItem(context)
	if err != nil {
		// handle error
		return
	}

	err = kvItem.SetKey("string_key")
	if err != nil {
		// handle error
		return
	}
	err = kvItem.SetValue("a1", int8(1))
	if err != nil {
		// handle error
		return
	}

	// Add the item to the kv store
	err = kv.AddItem(kvItem)
	if err != nil {
		// handle error
		return
	}

	// Flush will flush any in-memory kvitems to disk for persistance
	// Flush can be performance impacting if called to often due to syncing to disk
	err = kv.Flush()
	if err != nil {
		// handle error
		return
	}

	err = kv.Close()
	if err != nil {
		// handle error
		return
	}

	// Read key-value store
	err = kv.Open(TILEDB_READ)
	if err != nil {
		// handle error
		return
	}

	// Iterate of the kv store
	iter, err := kv.Iterate()
	if err != nil {
		// handle error
		return
	}

	for ; !iter.IsDone(); err = iter.Next() {
		if err != nil {
			// handle error from call to iter.Next()
			return
		}
		// Get current KVItem from iterator
		kvItemNew, err := iter.Here()
		if err != nil {
			// handle error
			return
		}
		key, err := kvItemNew.Key()
		if err != nil {
			// handle error
			return
		}
		a1, err := kvItemNew.Value("a1")
		if err != nil {
			// handle error
			return
		}
		// output: string_key
		// 1
		fmt.Println(key.(string))
		fmt.Println(a1.(int8))
	}
}

func CreateKVSchema(t *testing.T, context *Context) *KVSchema {

	kvSchema, err := NewKVSchema(context)
	assert.Nil(t, err)

	int8Attribute, err := NewAttribute(context, "int8Attribute", TILEDB_INT8)
	assert.Nil(t, err)
	int16Attribute, err := NewAttribute(context, "int16Attribute", TILEDB_INT16)
	assert.Nil(t, err)
	int32Attribute, err := NewAttribute(context, "int32Attribute", TILEDB_INT32)
	assert.Nil(t, err)
	int64Attribute, err := NewAttribute(context, "int64Attribute", TILEDB_INT64)
	assert.Nil(t, err)

	uint8Attribute, err := NewAttribute(context, "uint8Attribute", TILEDB_UINT8)
	assert.Nil(t, err)
	uint16Attribute, err := NewAttribute(context, "uint16Attribute", TILEDB_UINT16)
	assert.Nil(t, err)
	uint32Attribute, err := NewAttribute(context, "uint32Attribute", TILEDB_UINT32)
	assert.Nil(t, err)
	uint64Attribute, err := NewAttribute(context, "uint64Attribute", TILEDB_UINT64)
	assert.Nil(t, err)

	float32Attribute, err := NewAttribute(context, "float32Attribute", TILEDB_FLOAT32)
	assert.Nil(t, err)
	float64Attribute, err := NewAttribute(context, "float64Attribute", TILEDB_FLOAT64)
	assert.Nil(t, err)

	charAttribute, err := NewAttribute(context, "charAttribute", TILEDB_CHAR)
	assert.Nil(t, err)

	err = kvSchema.AddAttributes(int8Attribute, int16Attribute, int32Attribute, int64Attribute, uint8Attribute, uint16Attribute, uint32Attribute, uint64Attribute, float32Attribute, float64Attribute, charAttribute)
	assert.Nil(t, err)

	int8VarLengthAttribute, err := NewAttribute(context, "int8VarLengthAttribute", TILEDB_INT8)
	assert.Nil(t, err)
	int8VarLengthAttribute.SetCellValNum(TILEDB_VAR_NUM)
	int16VarLengthAttribute, err := NewAttribute(context, "int16VarLengthAttribute", TILEDB_INT16)
	assert.Nil(t, err)
	int16VarLengthAttribute.SetCellValNum(TILEDB_VAR_NUM)
	int32VarLengthAttribute, err := NewAttribute(context, "int32VarLengthAttribute", TILEDB_INT32)
	assert.Nil(t, err)
	int32VarLengthAttribute.SetCellValNum(TILEDB_VAR_NUM)
	int64VarLengthAttribute, err := NewAttribute(context, "int64VarLengthAttribute", TILEDB_INT64)
	assert.Nil(t, err)
	int64VarLengthAttribute.SetCellValNum(TILEDB_VAR_NUM)

	uint8VarLengthAttribute, err := NewAttribute(context, "uint8VarLengthAttribute", TILEDB_UINT8)
	assert.Nil(t, err)
	uint8VarLengthAttribute.SetCellValNum(TILEDB_VAR_NUM)
	uint16VarLengthAttribute, err := NewAttribute(context, "uint16VarLengthAttribute", TILEDB_UINT16)
	assert.Nil(t, err)
	uint16VarLengthAttribute.SetCellValNum(TILEDB_VAR_NUM)
	uint32VarLengthAttribute, err := NewAttribute(context, "uint32VarLengthAttribute", TILEDB_UINT32)
	assert.Nil(t, err)
	uint32VarLengthAttribute.SetCellValNum(TILEDB_VAR_NUM)
	uint64VarLengthAttribute, err := NewAttribute(context, "uint64VarLengthAttribute", TILEDB_UINT64)
	assert.Nil(t, err)
	uint64VarLengthAttribute.SetCellValNum(TILEDB_VAR_NUM)

	float32VarLengthAttribute, err := NewAttribute(context, "float32VarLengthAttribute", TILEDB_FLOAT32)
	assert.Nil(t, err)
	float32VarLengthAttribute.SetCellValNum(TILEDB_VAR_NUM)
	float64VarLengthAttribute, err := NewAttribute(context, "float64VarLengthAttribute", TILEDB_FLOAT64)
	assert.Nil(t, err)
	float64VarLengthAttribute.SetCellValNum(TILEDB_VAR_NUM)

	charVarLengthAttribute, err := NewAttribute(context, "charVarLengthAttribute", TILEDB_CHAR)
	assert.Nil(t, err)
	charVarLengthAttribute.SetCellValNum(TILEDB_VAR_NUM)

	err = kvSchema.AddAttributes(int8VarLengthAttribute, int16VarLengthAttribute, int32VarLengthAttribute, int64VarLengthAttribute, uint8VarLengthAttribute, uint16VarLengthAttribute, uint32VarLengthAttribute, uint64VarLengthAttribute, float32VarLengthAttribute, float64VarLengthAttribute, charVarLengthAttribute)
	assert.Nil(t, err)

	return kvSchema
}

func CreateKV(t *testing.T, context *Context) (*KV, string) {

	// create temp KV folder
	tmpKVPath := os.TempDir() + string(os.PathSeparator) + "tiledb_test_kv"
	if _, err := os.Stat(tmpKVPath); err == nil {
		os.RemoveAll(tmpKVPath)
	}

	kv, err := NewKV(context, tmpKVPath)
	assert.Nil(t, err)

	kvSchema := CreateKVSchema(t, context)
	assert.Nil(t, kvSchema.Check())

	err = kv.Create(kvSchema)
	assert.Nil(t, err)

	return kv, tmpKVPath
}

func TestKV(t *testing.T) {
	// Test context without config
	context, err := NewContext(nil)
	assert.Nil(t, err)

	kv, tmpKVPath := CreateKV(t, context)
	// Cleanup kv when test ends
	defer os.RemoveAll(tmpKVPath)

	assert.NotNil(t, kv)

	err = kv.OpenAt(TILEDB_READ, uint64(time.Now().UnixNano()/1000000))
	assert.Nil(t, err)

	err = kv.Reopen()
	assert.Nil(t, err)

	err = kv.ReopenAt(uint64(time.Now().UnixNano() / 1000000))
	assert.Nil(t, err)

	err = kv.Close()
	assert.Nil(t, err)

	err = kv.Open(TILEDB_WRITE)
	assert.Nil(t, err)

	kvItem, err := NewKVItem(context)
	assert.Nil(t, err)

	keys := []interface{}{int(1), "2"}

	err = kvItem.SetKey(keys[0])
	assert.Nil(t, err)
	err = kvItem.SetValue("int8Attribute", int8(1))
	assert.Nil(t, err)
	err = kvItem.SetValue("int16Attribute", int16(1))
	assert.Nil(t, err)
	err = kvItem.SetValue("int32Attribute", int32(1))
	assert.Nil(t, err)
	err = kvItem.SetValue("int64Attribute", int64(1))
	assert.Nil(t, err)
	err = kvItem.SetValue("uint8Attribute", uint8(1))
	assert.Nil(t, err)
	err = kvItem.SetValue("uint16Attribute", uint16(1))
	assert.Nil(t, err)
	err = kvItem.SetValue("uint32Attribute", uint32(1))
	assert.Nil(t, err)
	err = kvItem.SetValue("uint64Attribute", uint64(1))
	assert.Nil(t, err)
	err = kvItem.SetValue("float32Attribute", float32(1))
	assert.Nil(t, err)
	err = kvItem.SetValue("float64Attribute", float64(1))
	assert.Nil(t, err)
	err = kvItem.SetValue("charAttribute", "1")
	assert.Nil(t, err)

	err = kvItem.SetValue("int8VarLengthAttribute", []int8{1, 2})
	assert.Nil(t, err)
	err = kvItem.SetValue("int16VarLengthAttribute", []int16{1, 2})
	assert.Nil(t, err)
	err = kvItem.SetValue("int32VarLengthAttribute", []int32{1, 2})
	assert.Nil(t, err)
	err = kvItem.SetValue("int64VarLengthAttribute", []int64{1, 2})
	assert.Nil(t, err)
	err = kvItem.SetValue("uint8VarLengthAttribute", []uint8{1, 2})
	assert.Nil(t, err)
	err = kvItem.SetValue("uint16VarLengthAttribute", []uint16{1, 2})
	assert.Nil(t, err)
	err = kvItem.SetValue("uint32VarLengthAttribute", []uint32{1, 2})
	assert.Nil(t, err)
	err = kvItem.SetValue("uint64VarLengthAttribute", []uint64{1, 2})
	assert.Nil(t, err)
	err = kvItem.SetValue("float32VarLengthAttribute", []float32{1, 2})
	assert.Nil(t, err)
	err = kvItem.SetValue("float64VarLengthAttribute", []float64{1, 2})
	assert.Nil(t, err)
	err = kvItem.SetValue("charVarLengthAttribute", "123")
	assert.Nil(t, err)

	err = kv.AddItem(kvItem)
	assert.Nil(t, err)

	isDirty, err := kv.IsDirty()
	assert.Nil(t, err)
	assert.True(t, isDirty)

	err = kvItem.SetKey(keys[1])
	assert.Nil(t, err)

	err = kv.AddItem(kvItem)
	assert.Nil(t, err)

	isOpen, err := kv.IsOpen()
	assert.Nil(t, err)
	assert.True(t, isOpen)

	err = kv.Flush()
	assert.Nil(t, err)

	err = kv.Close()
	assert.Nil(t, err)

	err = kv.Open(TILEDB_READ)
	assert.Nil(t, err)

	// Iterate of the kv store
	iter, err := kv.Iterate()
	assert.Nil(t, err)
	index := 0
	for ; !iter.IsDone(); err = iter.Next() {
		assert.Nil(t, err)
		kvItemNew, err := iter.Here()
		assert.Nil(t, err)
		assert.NotNil(t, kvItemNew)

		key, err := kvItemNew.Key()
		assert.Nil(t, err)

		assert.EqualValues(t, keys[index], key)
		index++

		int8Value, err := kvItemNew.Value("int8Attribute")
		assert.Nil(t, err)
		assert.EqualValues(t, 1, int8Value)
		int16Value, err := kvItemNew.Value("int16Attribute")
		assert.Nil(t, err)
		assert.EqualValues(t, 1, int16Value)
		int32Value, err := kvItemNew.Value("int32Attribute")
		assert.Nil(t, err)
		assert.EqualValues(t, 1, int32Value)
		int64Value, err := kvItemNew.Value("int64Attribute")
		assert.Nil(t, err)
		assert.EqualValues(t, 1, int64Value)

		uint8Value, err := kvItemNew.Value("uint8Attribute")
		assert.Nil(t, err)
		assert.EqualValues(t, 1, uint8Value)
		uint16Value, err := kvItemNew.Value("uint16Attribute")
		assert.Nil(t, err)
		assert.EqualValues(t, 1, uint16Value)
		uint32Value, err := kvItemNew.Value("uint32Attribute")
		assert.Nil(t, err)
		assert.EqualValues(t, 1, uint32Value)
		uint64Value, err := kvItemNew.Value("uint64Attribute")
		assert.Nil(t, err)
		assert.EqualValues(t, 1, uint64Value)

		float32Value, err := kvItemNew.Value("float32Attribute")
		assert.Nil(t, err)
		assert.EqualValues(t, 1, float32Value)
		float64Value, err := kvItemNew.Value("float64Attribute")
		assert.Nil(t, err)
		assert.EqualValues(t, 1, float64Value)

		charValue, err := kvItemNew.Value("charAttribute")
		assert.Nil(t, err)
		assert.EqualValues(t, "1", charValue)

		int8VarLengthValue, err := kvItemNew.Value("int8VarLengthAttribute")
		assert.Nil(t, err)
		assert.EqualValues(t, []int8{1, 2}, int8VarLengthValue)
		int16VarLengthValue, err := kvItemNew.Value("int16VarLengthAttribute")
		assert.Nil(t, err)
		assert.EqualValues(t, []int16{1, 2}, int16VarLengthValue)
		int32VarLengthValue, err := kvItemNew.Value("int32VarLengthAttribute")
		assert.Nil(t, err)
		assert.EqualValues(t, []int32{1, 2}, int32VarLengthValue)
		int64VarLengthValue, err := kvItemNew.Value("int64VarLengthAttribute")
		assert.Nil(t, err)
		assert.EqualValues(t, []int64{1, 2}, int64VarLengthValue)

		uint8VarLengthValue, err := kvItemNew.Value("uint8VarLengthAttribute")
		assert.Nil(t, err)
		assert.EqualValues(t, []uint8{1, 2}, uint8VarLengthValue)
		uint16VarLengthValue, err := kvItemNew.Value("uint16VarLengthAttribute")
		assert.Nil(t, err)
		assert.EqualValues(t, []uint16{1, 2}, uint16VarLengthValue)
		uint32VarLengthValue, err := kvItemNew.Value("uint32VarLengthAttribute")
		assert.Nil(t, err)
		assert.EqualValues(t, []uint32{1, 2}, uint32VarLengthValue)
		uint64VarLengthValue, err := kvItemNew.Value("uint64VarLengthAttribute")
		assert.Nil(t, err)
		assert.EqualValues(t, []uint64{1, 2}, uint64VarLengthValue)

		float32VarLengthValue, err := kvItemNew.Value("float32VarLengthAttribute")
		assert.Nil(t, err)
		assert.EqualValues(t, []float32{1, 2}, float32VarLengthValue)
		float64VarLengthValue, err := kvItemNew.Value("float64VarLengthAttribute")
		assert.Nil(t, err)
		assert.EqualValues(t, []float64{1, 2}, float64VarLengthValue)

		charVarLengthValue, err := kvItemNew.Value("charVarLengthAttribute")
		assert.Nil(t, err)
		assert.EqualValues(t, "123", charVarLengthValue)

	}
}
