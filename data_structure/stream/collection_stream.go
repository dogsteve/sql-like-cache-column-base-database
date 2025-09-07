package stream

import (
	"runtime"
	"sort"
	"sync"
)

// StreamDataProvider is an interface for data sources that can be used in a stream.
// It allows the stream to iterate over data with offset and limit optimizations applied at the source.
type StreamDataProvider[T any] interface {
	// Range iterates over the data source, calling the provided function for each item.
	// The iteration should respect the offset and limit parameters.
	Range(consumer func(value T) bool, offset *uint64, limit *uint64)
}

// Stream represents a sequence of elements that can be processed.
type Stream[T any] struct {
	provider StreamDataProvider[T]
	ops      []interface{}
	limit    *uint64
	offset   *uint64
}

// From creates a new Stream from a compatible data provider.
func From[T any](provider StreamDataProvider[T]) *Stream[T] {
	return &Stream[T]{
		provider: provider,
	}
}

// Map adds a map operation to the stream.
func (stream *Stream[T]) Map(mapper func(T) T) *Stream[T] {
	stream.ops = append(stream.ops, mapper)
	return stream
}

// Filter adds a filter operation to the stream.
func (stream *Stream[T]) Filter(predicate func(T) bool) *Stream[T] {
	stream.ops = append(stream.ops, predicate)
	return stream
}

// Order adds a sort operation to the stream.
func (stream *Stream[T]) Order(less func(a, b T) bool) *Stream[T] {
	stream.ops = append(stream.ops, less)
	return stream
}

// Limit sets the maximum number of elements to be returned.
func (stream *Stream[T]) Limit(limit uint64) *Stream[T] {
	stream.limit = &limit
	return stream
}

// Offset sets the starting position of the elements to be returned.
func (stream *Stream[T]) Offset(offset uint64) *Stream[T] {
	stream.offset = &offset
	return stream
}

// parallelQuickSort performs an in-place parallel quick sort.
func parallelQuickSort[T any](data []T, less func(a, b T) bool) {
	if len(data) < 2 {
		return
	}
	const threshold = 2048 // Threshold for switching to sequential sort

	if len(data) < threshold {
		sort.Slice(data, func(i, j int) bool { return less(data[i], data[j]) })
		return
	}

	// Median-of-three pivot selection
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

	// Lomuto partition scheme
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

// Collect processes the stream and returns a slice of the results.
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

	// Pipeline stages are connected by channels
	filteredDataChannel := make(chan T, 100) // Buffered channel
	finalDataChannel := make(chan T, 100)    // Buffered channel

	// Stage 1: Data Provider -> Filtering
	go func() {
		defer close(filteredDataChannel)
		// The provider's Range method handles offset and limit at the source.
		stream.provider.Range(func(value T) bool {
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
			return true // Continue iteration
		}, stream.offset, stream.limit)
	}()

	// Stage 2: Mapping
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

	// Stage 3: Collecting
	result := make([]T, 0)
	for value := range finalDataChannel {
		result = append(result, value)
	}

	// Final Stage: Sorting
	if orderOperation != nil {
		parallelQuickSort(result, orderOperation)
	}

	return result
}
