package map_table

import (
	datastructure "a-eighty/data_structure/map"
	"fmt"
	"sync"
	"time"

	"github.com/bytedance/sonic"
	"github.com/google/uuid"
)

// DataReference holds a reference to the actual data along with metadata.
type DataReference[T any] struct {
	Index      int
	InsertTime time.Time
	Data       *T
}

type DataTable[T any] struct {
	mu                  sync.RWMutex
	sharedKey           string
	listData            []*DataReference[T]
	valueToReferenceMap datastructure.TTLMap[string, datastructure.TTLMap[string, []*DataReference[T]]]
}

func NewDataTable[T any]() *DataTable[T] {
	return &DataTable[T]{
		sharedKey:           uuid.NewString(),
		listData:            make([]*DataReference[T], 0),
		valueToReferenceMap: datastructure.TTLMap[string, datastructure.TTLMap[string, []*DataReference[T]]]{},
	}
}

// Insert thêm một bản ghi mới vào ma trận dữ liệu.
func (tdm *DataTable[T]) Insert(data T, ttl time.Duration) error {
	tdm.mu.Lock()
	defer tdm.mu.Unlock()

	jsonValue, err := sonic.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal data to JSON: %w", err)
	}

	var objectStructureMap map[string]interface{}
	err = sonic.Unmarshal(jsonValue, &objectStructureMap)
	if err != nil {
		return fmt.Errorf("failed to unmarshal JSON to map: %w", err)
	}

	index := len(tdm.listData)
	dataRef := &DataReference[T]{
		Index:      index,
		InsertTime: time.Now(),
		Data:       &data,
	}

	tdm.listData = append(tdm.listData, dataRef)

	for key, value := range objectStructureMap {
		valueString := fmt.Sprintf("%v", value)

		if innerValueMap, ok := tdm.valueToReferenceMap.Get(key); !ok {
			dataList := []*DataReference[T]{dataRef}
			newInnerValueMap := datastructure.NewTTLMap[string, []*DataReference[T]]()
			newInnerValueMap.Set(valueString, &dataList, -1)
			tdm.valueToReferenceMap.Set(key, newInnerValueMap, -1)
		} else {
			dataList, _ := innerValueMap.Get(valueString)
			*dataList = append(*dataList, dataRef)
			innerValueMap.Set(valueString, dataList, -1)
		}
	}

	return nil
}

func (tdm *DataTable[T]) GetDataByFieldValue(fieldName, value string) ([]*T, bool) {
	if fieldMap, ok := tdm.valueToReferenceMap[fieldName]; ok {
		if dataRefs, ok := fieldMap[value]; ok {
			result := make([]*T, len(dataRefs))
			for i, dataRef := range dataRefs {
				result[i] = dataRef.Data
			}
			return result, true
		}
	}
	return nil, false
}

func (tdm *DataTable[T]) GetDataByIndex(index int) (*T, bool) {
	if index >= 0 && index < len(tdm.listData) {
		return tdm.listData[index].Data, true
	}
	return nil, false
}
