package test

import (
	"a-eighty/data_structure/slice"
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

}
