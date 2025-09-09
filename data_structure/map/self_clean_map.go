package map_data_structure

import (
	"sync"
	"time"
	"unsafe"
)

type Item[V any] struct {
	value      *V
	expiration int64
}

type TTLMap[K any, V any] struct {
	innerMap sync.Map
}

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

/*func (ttlMap *TTLMap[K, V]) SortKeys() []K {
	var keys []K
	ttlMap.innerMap.Range(func(key, value any) bool {
		keys = append(keys, key)
		return true
	})

	sort.Slice(keys, func(i, j int) bool {

		keyI := any(keys[i])
		keyJ := any(keys[j])
		switch vI := keyI.(type) {
		case int:
			if vJ, ok := keyJ.(int); ok {
				return vI < vJ
			}

			return true
		case string:
			if vJ, ok := keyJ.(string); ok {
				return vI < vJ
			}

			return false
		default:

			return false
		}
	})
	return keys
}*/

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

func (ttlMap *TTLMap[K, V]) Get(key K) (*V, bool) {
	val, ok := ttlMap.innerMap.Load(key)
	if !ok {
		var zero V
		return &zero, false
	}
	item := val.(Item[V])
	if item.expiration != -1 && time.Now().UnixNano() > item.expiration {
		ttlMap.innerMap.Delete(key)
		var zero V
		return &zero, false
	}
	return item.value, true
}

func (ttlMap *TTLMap[K, V]) Delete(key K) {
	ttlMap.innerMap.Delete(key)
}

func (ttlMap *TTLMap[K, V]) Len() int {
	var length int
	ttlMap.innerMap.Range(func(key, value any) bool {
		length++
		return true
	})
	return length
}

func (ttlMap *TTLMap[K, V]) Items(consumer func(key K, value *V) bool) {
	ttlMap.innerMap.Range(func(k, v any) bool {
		item := v.(Item[V])
		if item.expiration != -1 && time.Now().UnixNano() > item.expiration {
			ttlMap.innerMap.Delete(k)
			return true
		}
		return consumer(k.(K), item.value)
	})
}

func (ttlMap *TTLMap[K, V]) Range(consumer func(index int, value V) bool, offset *uint64, limit *uint64) {
	now := time.Now().UnixNano()
	keys := make([]K, 0)

	ttlMap.innerMap.Range(func(k, v any) bool {
		item := v.(Item[V])
		if item.expiration == -1 || now <= item.expiration {
			keys = append(keys, k.(K))
		}
		return true
	})

	var start uint64 = 0
	if offset != nil {
		start = *offset
	}
	keys = keys[start:]

	if limit != nil {
		lim := *limit
		if lim < uint64(len(keys)) {
			keys = keys[:lim]
		}
	}
	for i, k := range keys {
		if val, ok := ttlMap.innerMap.Load(k); ok {
			item := val.(Item[V])
			if item.expiration == -1 || now <= item.expiration {
				if !consumer(i, *item.value) {
					break
				}
			}
		}
	}
}

func (ttlMap *TTLMap[K, V]) DeleteAll(predicate func(value V) bool) {
	ttlMap.innerMap.Range(func(key, value any) bool {
		item := value.(Item[V])
		if predicate(*item.value) {
			ttlMap.innerMap.Delete(key)
		}
		return true
	})
}

func registerTTLMap[K any, V any](m *TTLMap[K, V]) {
	for {
		oldSlice := atomicRegistry.Load()
		newSlice := append(*oldSlice, (*TTLMap[any, any])(unsafe.Pointer(m)))
		if atomicRegistry.CompareAndSwap(oldSlice, &newSlice) {
			break
		}
	}
}
