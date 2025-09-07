package data_element

type DataQueryFunc[T any] interface {
	Insert(data T) error
	GetDataByFieldValue(fieldName, value string) ([]*T, bool)
	GetDataByIndex(index int) (*T, bool)
}

type Condition struct {
	Field     string
	Operation string
	Value     string
	Values    []string
}

type QueryBuilder[T any] struct {
	table         *DataTable[T]
	orConditions  []Condition
	andConditions []Condition
	limit         int
	offset        int
}

func NewQueryBuilder[T any](table *DataTable[T]) *QueryBuilder[T] {
	return &QueryBuilder[T]{
		table:         table,
		orConditions:  make([]Condition, 0),
		andConditions: make([]Condition, 0),
		limit:         -1,
		offset:        -1,
	}
}

func (qb *QueryBuilder[T]) Or(conditions ...Condition) *QueryBuilder[T] {
	qb.orConditions = append(qb.orConditions, conditions...)
	return qb
}

func (qb *QueryBuilder[T]) And(conditions ...Condition) *QueryBuilder[T] {
	qb.andConditions = append(qb.andConditions, conditions...)
	return qb
}

func (qb *QueryBuilder[T]) Limit(limit int) *QueryBuilder[T] {
	qb.limit = limit
	return qb
}

func (qb *QueryBuilder[T]) Offset(offset int) *QueryBuilder[T] {
	qb.offset = offset
	return qb
}

func (QueryBuilder[T]) ParseTable(tableName string) ([]*T, error) {

}
