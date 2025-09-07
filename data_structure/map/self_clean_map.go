package map_data_structure

import (
	"sync"
	"time"
	"unsafe"
)

// Item represents a key-value pair with an expiration timestamp.
type Item[V any] struct {
	value      *V
	expiration int64
}

// TTLMap is a thread-safe map with a time-to-live for each item.
// It is now managed by a single, global cleanup service.
type TTLMap[K any, V any] struct {
	innerMap sync.Map
}

// cleanExpiredItems is the internal method for a single TTLMap.
// It iterates and removes expired items.
func (ttlMap *TTLMap[K, V]) cleanExpiredItems() {
	now := time.Now().UnixNano()
	ttlMap.innerMap.Range(func(key, val any) bool {
		item := val.(Item[V])
		if item.expiration != -1 && now > item.expiration {
			ttlMap.innerMap.Delete(key)
		}
		return true
	})
}

func NewTTLMap[K any, V any]() *TTLMap[K, V] {
	m := &TTLMap[K, V]{}
	registerTTLMap(m)
	return m
}

// Set adds or updates a key-value pair with a specified TTL.
func (ttlMap *TTLMap[K, V]) Set(key K, value *V, ttl time.Duration) {
	var expiration int64
	if ttl == -1 {
		expiration = -1
	} else {
		expiration = time.Now().Add(ttl).UnixNano()
	}

	ttlMap.innerMap.Store(key, Item[V]{
		value:      value,
		expiration: expiration,
	})
}

// Get retrieves a value from the map if it is still valid.
func (ttlMap *TTLMap[K, V]) Get(key K) (*V, bool) {
	val, ok := ttlMap.innerMap.Load(key)
	if !ok {
		var zero V
		return &zero, false
	}
	item := val.(Item[V])
	if item.expiration != -1 && time.Now().UnixNano() > item.expiration {
		ttlMap.innerMap.Delete(key) // Lazily delete expired items on access.
		var zero V
		return &zero, false
	}
	return item.value, true
}

// Delete removes a key-value pair from the map.
func (ttlMap *TTLMap[K, V]) Delete(key K) {
	ttlMap.innerMap.Delete(key)
}

// Len returns the number of items in the map.
func (ttlMap *TTLMap[K, V]) Len() uint {
	var length uint
	ttlMap.innerMap.Range(func(key, value any) bool {
		length++
		return true
	})
	return length
}

// Items returns a snapshot of all valid items in the map.
func (ttlMap *TTLMap[K, V]) Items(consumer func(key K, value V) bool) {
	ttlMap.innerMap.Range(func(k, v any) bool {
		item := v.(Item[V])
		if item.expiration != -1 && time.Now().UnixNano() > item.expiration {
			ttlMap.innerMap.Delete(k) // Lazily delete expired items on access.
			return true
		}
		return consumer(k.(K), *item.value)
	})
}

// Range iterates over the map items in a deterministic order, applying offset and limit.
// This makes TTLMap compatible with the stream API.
func (ttlMap *TTLMap[K, V]) Range(consumer func(value V) bool, offset *uint64, limit *uint64) {
	// 1. Collect all valid keys
	now := time.Now().UnixNano()
	keys := make([]K, 0)
	ttlMap.innerMap.Range(func(k, v any) bool {
		item := v.(Item[V])
		if item.expiration == -1 || now <= item.expiration {
			keys = append(keys, k.(K))
		}
		return true
	})

	//// 2. Sort keys for deterministic iteration
	//sort.Slice(keys, func(i, j int) bool {
	//	return fmt.Sprint(keys[i]) < fmt.Sprint(keys[j])
	//})

	// 3. Apply offset
	var start uint64 = 0
	if offset != nil {
		start = *offset
	}
	if start >= uint64(len(keys)) {
		return // offset is out of bounds
	}
	keys = keys[start:]

	// 4. Apply limit
	if limit != nil {
		lim := *limit
		if lim < uint64(len(keys)) {
			keys = keys[:lim]
		}
	}

	// 5. Iterate over the final set of keys and call the function
	for _, k := range keys {
		if val, ok := ttlMap.innerMap.Load(k); ok {
			item := val.(Item[V])
			// Double-check expiration in case it expired during the process
			if item.expiration == -1 || now <= item.expiration {
				if !consumer(*item.value) {
					break
				}
			}
		}
	}
}

// registerTTLMap adds a new TTLMap to the global registry using atomic operations.
func registerTTLMap[K any, V any](m *TTLMap[K, V]) {
	for {
		oldSlice := atomicRegistry.Load()
		newSlice := append(*oldSlice, (*TTLMap[any, any])(unsafe.Pointer(m)))
		if atomicRegistry.CompareAndSwap(oldSlice, &newSlice) {
			break
		}
	}
}
