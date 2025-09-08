package stream

import (
	"math"
	"runtime"
	"sort"
	"sync"
)

type StreamDataProvider[T any] interface {
	Range(consumer func(index int, value T) bool, offset *uint64, limit *uint64)
}

type Stream[T any] struct {
	provider StreamDataProvider[T]
	ops      []interface{}
	limit    *uint64
	offset   *uint64
}

func From[T any](provider StreamDataProvider[T]) *Stream[T] {
	return &Stream[T]{
		provider: provider,
	}
}

func (stream *Stream[T]) Map(mapper func(T) T) *Stream[T] {
	stream.ops = append(stream.ops, mapper)
	return stream
}

func (stream *Stream[T]) Filter(predicate func(T) bool) *Stream[T] {
	stream.ops = append(stream.ops, predicate)
	return stream
}

func (stream *Stream[T]) Sort(less func(a, b T) bool) *Stream[T] {
	stream.ops = append(stream.ops, less)
	return stream
}

func (stream *Stream[T]) Limit(limit *uint64) *Stream[T] {
	if limit == nil {
		var maxVal uint64 = 999999999
		stream.limit = &maxVal
	} else {
		stream.limit = limit
	}
	return stream
}

func (stream *Stream[T]) Offset(offset *uint64) *Stream[T] {
	if offset == nil {
		var zeroVal uint64 = 0
		stream.offset = &zeroVal
	} else {
		stream.offset = offset
	}
	return stream
}

func parallelQuickSort[T any](data []T, less func(a, b T) bool) {
	if len(data) < 2 {
		return
	}
	const threshold = math.MaxInt

	if len(data) < threshold {
		sort.Slice(data, func(i, j int) bool { return less(data[i], data[j]) })
		return
	}

	mid := len(data) / 2
	last := len(data) - 1
	if less(data[mid], data[0]) {
		data[0], data[mid] = data[mid], data[0]
	}
	if less(data[last], data[0]) {
		data[0], data[last] = data[last], data[0]
	}
	if less(data[mid], data[last]) {
		data[mid], data[last] = data[last], data[mid]
	}

	pivotIndex := 0
	for j := 0; j < last; j++ {
		if less(data[j], data[last]) {
			data[pivotIndex], data[j] = data[j], data[pivotIndex]
			pivotIndex++
		}
	}
	data[pivotIndex], data[last] = data[last], data[pivotIndex]

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		parallelQuickSort[T](data[:pivotIndex], less)
	}()
	go func() {
		defer wg.Done()
		parallelQuickSort[T](data[pivotIndex+1:], less)
	}()
	wg.Wait()
}

func (stream *Stream[T]) Collect() []T {
	filterOperations := make([]func(T) bool, 0)
	mappingOperations := make([]func(T) T, 0)
	var orderOperation func(a, b T) bool

	for _, op := range stream.ops {
		switch o := op.(type) {
		case func(T) bool:
			filterOperations = append(filterOperations, o)
		case func(T) T:
			mappingOperations = append(mappingOperations, o)
		case func(a, b T) bool:
			orderOperation = o
		}
	}

	filteredDataChannel := make(chan T, 100)
	finalDataChannel := make(chan T, 100)

	go func() {
		defer close(filteredDataChannel)

		stream.provider.Range(func(index int, value T) bool {
			isMatch := true
			for _, p := range filterOperations {
				if !p(value) {
					isMatch = false
					break
				}
			}
			if isMatch {
				filteredDataChannel <- value
			}
			return true
		}, stream.offset, stream.limit)
	}()

	go func() {
		defer close(finalDataChannel)
		var wg sync.WaitGroup
		numWorkers := runtime.NumCPU()
		wg.Add(numWorkers)

		for i := 0; i < numWorkers; i++ {
			go func() {
				defer wg.Done()
				for value := range filteredDataChannel {
					mappedValue := value
					for _, m := range mappingOperations {
						mappedValue = m(mappedValue)
					}
					finalDataChannel <- mappedValue
				}
			}()
		}
		wg.Wait()
	}()

	result := make([]T, 0)
	for value := range finalDataChannel {
		result = append(result, value)
	}

	if orderOperation != nil {
		parallelQuickSort(result, orderOperation)
	}

	return result
}
