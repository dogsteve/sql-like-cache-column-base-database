package map_table

import (
	datastructure "a-eighty/data_structure/map"
	data_structure_slice "a-eighty/data_structure/slice"
	"a-eighty/data_structure/stream"
	"fmt"
	"time"

	"github.com/bytedance/sonic"
	"github.com/google/uuid"
)

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
	valueToReferenceMap datastructure.TTLMap[string, datastructure.TTLMap[string, data_structure_slice.TTLSlice[map[string]any]]]
}

func NewDataTable(tableName string) *DataTable {
	if tableName == "" {
		tableName = "unknown"
	}
	return &DataTable{
		sharedKey:           uuid.NewString(),
		listData:            *data_structure_slice.NewTTLSlice[map[string]any](),
		valueToReferenceMap: *datastructure.NewTTLMap[string, datastructure.TTLMap[string, data_structure_slice.TTLSlice[map[string]any]]](),
	}
}

func (tdm *DataTable) Insert(data map[string]any, ttl time.Duration) error {
	jsonValue, err := sonic.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal data to JSON: %w", err)
	}

	var objectStructureMap map[string]interface{}
	err = sonic.Unmarshal(jsonValue, &objectStructureMap)
	if err != nil {
		return fmt.Errorf("failed to unmarshal JSON to map: %w", err)
	}

	tdm.listData.Append(data, ttl)

	for key, value := range objectStructureMap {
		valueString := fmt.Sprintf("%v", value)

		if innerValueMap, ok := tdm.valueToReferenceMap.Get(key); !ok {
			dataList := data_structure_slice.NewTTLSlice[map[string]any]()
			dataList.Append(data, ttl)
			newInnerValueMap := datastructure.NewTTLMap[string, data_structure_slice.TTLSlice[map[string]any]]()
			newInnerValueMap.Set(valueString, dataList, ttl)
			tdm.valueToReferenceMap.Set(key, newInnerValueMap, ttl)
		} else {
			dataList, _ := innerValueMap.Get(valueString)
			dataList.Append(data, ttl)
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
	filteredValues := make([]map[string]any, 0)
	tdm.valueToReferenceMap.Items(func(parentKey string, mapValue *datastructure.TTLMap[string, data_structure_slice.TTLSlice[map[string]any]]) bool {
		mapValue.Items(func(key string, sliceValue *data_structure_slice.TTLSlice[map[string]any]) bool {
			builtMap := map[string]any{
				parentKey: key,
			}
			if predicate(builtMap) {
				filteredValues = append(filteredValues, sliceValue.GetAll()...)
			}
			return true
		})
		return true
	})
	wrappedArray := data_structure_slice.ArraySlice[map[string]any]{
		Array: filteredValues,
	}
	dataStream := stream.From[map[string]any](&wrappedArray)
	return dataStream.Sort(sort).Limit(limit).Offset(offset).Collect()
}

func (tdm *DataTable) Delete(predicate func(map[string]any) bool) error {
	tdm.listData.DeleteAll(predicate)
	tdm.valueToReferenceMap.Items(func(parentKey string, mapValue *datastructure.TTLMap[string, data_structure_slice.TTLSlice[map[string]any]]) bool {
		mapValue.Items(func(key string, _ *data_structure_slice.TTLSlice[map[string]any]) bool {
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
