package data_structure_slice

type ArraySlice[T any] struct {
	Array []T
}

func (arraySlice *ArraySlice[T]) Range(consumer func(index int, value T) bool, offset *uint64, limit *uint64) {
	innerArr := arraySlice.Array
	var start uint64 = 0
	if offset != nil {
		start = *offset
	}
	if start >= uint64(len(innerArr)) {
		return
	}
	innerArr = innerArr[start:]

	if limit != nil {
		lim := *limit
		if lim < uint64(len(innerArr)) {
			innerArr = innerArr[:lim]
		}
	}

	for i, k := range innerArr {
		consumer(i, k)
	}
}
