package data_structure_slice

import (
	map_data_structure "a-eighty/data_structure/map"
	"sync"
	"time"
)

// TTLSlice implements a concurrent, auto-expiring slice using atomic operations.
type TTLSlice[T any] struct {
	rwMutex  sync.RWMutex
	innerMap map_data_structure.TTLMap[uint, T]
}

// NewTTLSlice creates a new instance and registers it with the global cleaner.
func NewTTLSlice[T any]() *TTLSlice[T] {
	return &TTLSlice[T]{
		rwMutex:  sync.RWMutex{},
		innerMap: *map_data_structure.NewTTLMap[uint, T](),
	}
}

// Append adds a new value with a specified key and TTL.
func (mainSlice *TTLSlice[T]) Append(value T, ttl time.Duration) {
	mainSlice.rwMutex.Lock()
	defer mainSlice.rwMutex.Unlock()
	mapLength := mainSlice.innerMap.Len()
	mainSlice.innerMap.Set(mapLength+1, &value, ttl)
}

// Delete removes an item by its key.
func (mainSlice *TTLSlice[T]) Delete(index uint) {
	mainSlice.innerMap.Delete(index)
}

// Get returns the value associated with a key.
func (mainSlice *TTLSlice[T]) Get(index uint) (*T, bool) {
	return mainSlice.innerMap.Get(index)
}

// GetAll returns all valid values in a new slice.
func (mainSlice *TTLSlice[T]) GetAll() []T {
	var result []T = make([]T, 0)
	// Iterate over all items in the inner map and collect valid ones.
	mainSlice.innerMap.Items(func(key uint, value T) bool {
		result = append(result, value)
		return true
	})
	return result
}

// GetAll returns all valid values in a new slice.
func (mainSlice *TTLSlice[T]) Range(consumer func(value T) bool, offset *uint64, limit *uint64) {
	mainSlice.innerMap.Range(consumer, offset, limit)
}
