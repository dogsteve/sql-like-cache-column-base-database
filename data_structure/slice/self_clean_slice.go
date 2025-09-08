package data_structure_slice

import (
	map_data_structure "a-eighty/data_structure/map"
	"time"
)

type TTLSlice[T any] struct {
	innerMap map_data_structure.TTLMap[int, T]
}

func NewTTLSlice[T any]() *TTLSlice[T] {
	return &TTLSlice[T]{
		innerMap: *map_data_structure.NewTTLMap[int, T](),
	}
}

func (mainSlice *TTLSlice[T]) Append(value T, ttl time.Duration) {
	mapLength := mainSlice.innerMap.Len()
	mainSlice.innerMap.Set(mapLength+1, &value, ttl)
}

func (mainSlice *TTLSlice[T]) Delete(index int) {
	mainSlice.innerMap.Delete(index)
}

func (mainSlice *TTLSlice[T]) Get(index int) (*T, bool) {
	return mainSlice.innerMap.Get(index)
}

func (mainSlice *TTLSlice[T]) GetAll() []T {
	result := make([]T, 0)
	mainSlice.innerMap.Items(func(key int, value *T) bool {
		result = append(result, *value)
		return true
	})
	return result
}

func (mainSlice *TTLSlice[T]) Range(consumer func(index int, value T) bool, offset *uint64, limit *uint64) {
	mainSlice.innerMap.Range(consumer, offset, limit)
}

func (mainSlice *TTLSlice[T]) Len() int {
	return mainSlice.innerMap.Len()
}

func (mainSlice *TTLSlice[T]) DeleteAll(predicate func(value T) bool) {
	if predicate == nil {
		mainSlice.innerMap.Range(func(index int, value T) bool {
			mainSlice.Delete(index)
			return true
		}, nil, nil)
	} else {
		mainSlice.innerMap.Range(func(index int, value T) bool {
			if predicate(value) {
				mainSlice.Delete(index)
			}
			return true
		}, nil, nil)
	}
}
