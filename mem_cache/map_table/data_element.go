package map_table

import (
	datastructure "a-eighty/data_structure/map"
	data_structure_slice "a-eighty/data_structure/slice"
	"a-eighty/data_structure/stream"
	"a-eighty/utils"
	"time"

	"github.com/google/uuid"
)

type WrapperNode struct {
	Index int
	Value map[string]any
}

type DataTable struct {
	tableName string
	sharedKey string
	listData  data_structure_slice.TTLSlice[map[string]any]
	/*
		there are 3 objects below going to insert into table
		object1 = {
				"field1": "val1",
				"field2": "val2",
				"field3": "val3",
			}
		object2 = {
				"field1": "val1",
				"field2": "val1",
				"field3": "val3",
			}
		object3 = {
				"field1": "val1",
				"field2": "val1",
				"field3": "val2",
			}
		data map store in map look like this
		"filed1":
			"val1" -> objet1, object2, object3
		"field2":
			"val1" -> object2, object3
			"val2" -> object1
		"field3":
			"val3" -> object1, object2
			"val2" -> object3
	*/
	valueToReferenceMap datastructure.TTLMap[string, datastructure.TTLMap[any, data_structure_slice.TTLSlice[WrapperNode]]]
}

func NewDataTable(tableName string) *DataTable {
	if tableName == "" {
		tableName = "unknown"
	}
	return &DataTable{
		sharedKey:           uuid.NewString(),
		listData:            *data_structure_slice.NewTTLSlice[map[string]any](),
		valueToReferenceMap: *datastructure.NewTTLMap[string, datastructure.TTLMap[any, data_structure_slice.TTLSlice[WrapperNode]]](),
	}
}

func (tdm *DataTable) Insert(data map[string]any, ttl time.Duration) error {

	tdm.listData.Append(data, ttl)
	lastedIndex := tdm.listData.Len()

	for key, value := range data {
		wrappedNode := WrapperNode{
			Index: lastedIndex,
			Value: data,
		}
		if innerValueMap, ok := tdm.valueToReferenceMap.Get(key); !ok {

			newDataList := data_structure_slice.NewTTLSlice[WrapperNode]()
			newDataList.Append(wrappedNode, ttl)
			newInnerValueMap := datastructure.NewTTLMap[any, data_structure_slice.TTLSlice[WrapperNode]]()
			newInnerValueMap.Set(value, newDataList, -1)
			tdm.valueToReferenceMap.Set(key, newInnerValueMap, -1)
		} else {

			containedFieldValueMap, contain := innerValueMap.Get(value)
			if contain {
				containedFieldValueMap.Append(wrappedNode, ttl)
			} else {
				containedFieldValueMap = data_structure_slice.NewTTLSlice[WrapperNode]()
				containedFieldValueMap.Append(wrappedNode, ttl)
				innerValueMap.Set(value, containedFieldValueMap, -1)
			}
		}
	}

	return nil
}

func (tdm *DataTable) GetDataByIndex(index int) (map[string]any, bool) {
	if value, isOk := tdm.listData.Get(index); isOk {
		return *value, true
	}
	return nil, false
}

func (tdm *DataTable) QueryWithCriteria(predicate func(map[string]any) bool, sort func(a, b map[string]any) bool, limit, offset *uint64) []map[string]any {
	filteredValuesMap := make(map[uint64]map[string]any)
	tdm.valueToReferenceMap.Items(func(parentKey string, mapValue *datastructure.TTLMap[any, data_structure_slice.TTLSlice[WrapperNode]]) bool {
		mapValue.Items(func(key any, sliceValue *data_structure_slice.TTLSlice[WrapperNode]) bool {
			builtMap := map[string]any{
				parentKey: key,
			}
			if predicate(builtMap) {
				sliceValue.Range(func(index int, value WrapperNode) bool {
					val := value.Value
					hashVal, err := utils.HashObject_XXHash(val)
					if err != nil {
						return true
					}
					filteredValuesMap[hashVal] = val
					return true
				}, nil, nil)
			}
			return true
		})
		return true
	})
	var filteredValues = make([]map[string]any, 0)
	for _, val := range filteredValuesMap {
		filteredValues = append(filteredValues, val)
	}

	wrappedArray := data_structure_slice.ArraySlice[map[string]any]{
		Array: filteredValues,
	}
	dataStream := stream.From[map[string]any](&wrappedArray)
	return dataStream.Sort(sort).Limit(limit).Offset(offset).Collect()
}

func (tdm *DataTable) Delete(predicate func(map[string]any) bool) error {
	tdm.listData.DeleteAll(predicate)
	tdm.valueToReferenceMap.Items(func(parentKey string, mapValue *datastructure.TTLMap[any, data_structure_slice.TTLSlice[WrapperNode]]) bool {
		mapValue.Items(func(key any, _ *data_structure_slice.TTLSlice[WrapperNode]) bool {
			builtMap := map[string]any{
				parentKey: key,
			}
			if predicate(builtMap) {
				mapValue.Delete(key)
			}
			return true
		})
		return true
	})
	return nil
}
