package data_query

import (
	"a-eighty/mem_cache/map_table"
	"errors"
	"fmt"
	"strconv"

	"vitess.io/vitess/go/vt/sqlparser"
)

func HandleSelect(databaseName string, selectStmt *sqlparser.Select) ([]map[string]any, error) {
	tableName := selectStmt.From[0].(*sqlparser.AliasedTableExpr)
	tableNameString := tableName.TableNameString()
	table, err := map_table.GetTable(databaseName, tableNameString)
	if err != nil {
		return nil, err
	}

	// 3. Handle WHERE clause using the builder from where.go.
	predicateFunction := func(map[string]any) bool {
		return true
	}
	if selectStmt.Where != nil {
		predicateFunction, err = BuildPredicateFromExpr[map[string]any](selectStmt.Where.Expr)
		if err != nil {
			return nil, fmt.Errorf("failed to build WHERE clause predicate: %w", err)
		}
	}

	// 4. Handle ORDER BY clause.
	sortFunction := func(a, b map[string]any) bool {
		return true
	}
	if len(selectStmt.OrderBy) > 0 {
		if len(selectStmt.OrderBy) > 1 {
			return nil, errors.New("ORDER BY with multiple columns is not currently supported")
		}
		order := selectStmt.OrderBy[0]
		colName, ok := order.Expr.(*sqlparser.ColName)
		if !ok {
			return nil, errors.New("ORDER BY expression must be a column name")
		}
		fieldName := colName.Name.String()
		isDesc := order.Direction == sqlparser.DescOrder

		sortFunction = func(a, b map[string]any) bool {
			mapA, errA := objectToMap(a)
			mapB, errB := objectToMap(b)
			if errA != nil || errB != nil {
				return false // Cannot compare if conversion fails.
			}

			valA, okA := mapA[fieldName]
			valB, okB := mapB[fieldName]
			if !okA || !okB {
				return false // Cannot compare if field is missing.
			}

			// Generic comparison logic.
			var isLess bool
			switch vA := valA.(type) {
			case float64:
				if vB, ok := valB.(float64); ok {
					isLess = vA < vB
				}
			case string:
				if vB, ok := valB.(string); ok {
					isLess = vA < vB
				}
			default:
				// Fallback to string comparison for other types.
				isLess = fmt.Sprint(valA) < fmt.Sprint(valB)
			}

			if isDesc {
				return !isLess
			}
			return isLess
		}
	}

	// 5. Handle LIMIT and OFFSET.
	var limitVal *uint64 = nil
	var offsetVal *uint64 = nil
	if selectStmt.Limit != nil {
		if selectStmt.Limit.Offset != nil {
			parsedVal, err := strconv.ParseUint(sqlparser.String(selectStmt.Limit.Offset), 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid OFFSET value: %w", err)
			}
			offsetVal = &parsedVal
		}
		parsedVal, err := strconv.ParseUint(sqlparser.String(selectStmt.Limit.Rowcount), 10, 64)
		limitVal = &parsedVal
		if err != nil {
			return nil, fmt.Errorf("invalid LIMIT value: %w", err)
		}
	}

	// 6. Execute the stream and return the results.
	result := table.QueryWithCriteria(predicateFunction, sortFunction, limitVal, offsetVal)
	return result, nil
}

// sliceDataProvider is a lightweight implementation of the StreamDataProvider interface that wraps a simple slice.
// This is essential for subquery execution.
type sliceDataProvider[T any] struct {
	data []T
}

// Range iterates over the slice, applying offset and limit.
func (s *sliceDataProvider[T]) Range(f func(value T) bool, offset *uint64, limit *uint64) {
	items := s.data

	var start uint64 = 0
	if offset != nil {
		start = *offset
	}
	if start >= uint64(len(items)) {
		return // Offset is out of bounds.
	}
	items = items[start:]

	if limit != nil {
		lim := *limit
		if lim < uint64(len(items)) {
			items = items[:lim]
		}
	}

	for _, item := range items {
		if !f(item) {
			break
		}
	}
}
