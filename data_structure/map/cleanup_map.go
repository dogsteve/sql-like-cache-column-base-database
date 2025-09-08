package map_data_structure

import (
	"sync/atomic"
	"time"
)

var (
	atomicRegistry atomic.Pointer[[]*TTLMap[any, any]]
)

func init() {
	atomicRegistry.Store(&[]*TTLMap[any, any]{})
}

func StartGlobalCleaner() {
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		for range ticker.C {
			registrySlice := atomicRegistry.Load()
			mapsToClean := *registrySlice
			for _, m := range mapsToClean {
				m.cleanExpiredItems()
			}
		}
	}()
}
