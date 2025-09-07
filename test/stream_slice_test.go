package test

import (
	"a-eighty/data_structure/slice"
	"a-eighty/data_structure/stream"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"
)

func TestStream(t *testing.T) {
	slice := data_structure_slice.NewTTLSlice[int]()
	numWorkers := runtime.NumCPU()
	var wg sync.WaitGroup
	wg.Add(numWorkers)
	for i := 0; i < numWorkers; i++ {
		go func() {
			defer wg.Done()
			for i := 0; i < 5000; i++ {
				slice.Append(i, time.Hour)
			}
		}()
	}
	wg.Wait()

	ttlSlice := stream.From[int](slice)
	result := ttlSlice.
		Filter(func(value int) bool {
			return value%2 == 0
		}).
		Map(func(value int) int {
			fmt.Println("mapping value", value)
			return value * 10
		}).
		Order(func(a, b int) bool {
			return a < b
		}).
		Limit(10).
		Offset(0).
		Collect()

	fmt.Println(len(result))
}
