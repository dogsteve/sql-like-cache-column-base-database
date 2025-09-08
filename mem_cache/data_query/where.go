package data_query

import (
	"fmt"
	"strconv"

	"github.com/bytedance/sonic"
	"vitess.io/vitess/go/vt/sqlparser"
)

// BuildPredicateFromExpr creates a filter predicate function from a SQL WHERE clause expression (AST).
// It uses a map-based comparison for flexibility, converting the input object into a map to evaluate conditions.
func BuildPredicateFromExpr[T any](expr sqlparser.Expr) (func(T) bool, error) {
	mapPredicate, err := buildMapPredicate(expr)
	if err != nil {
		return nil, err
	}

	// The final predicate function converts the object to a map and then evaluates it.
	return func(obj T) bool {
		objMap, err := objectToMap(obj)
		if err != nil {
			// If conversion fails, the object cannot be matched.
			return false
		}
		return mapPredicate(objMap)
	}, nil
}

// buildMapPredicate is the recursive core that builds a predicate for a map.
func buildMapPredicate(expr sqlparser.Expr) (func(map[string]interface{}) bool, error) {
	switch e := expr.(type) {
	case *sqlparser.AndExpr:
		left, err := buildMapPredicate(e.Left)
		if err != nil {
			return nil, err
		}
		right, err := buildMapPredicate(e.Right)
		if err != nil {
			return nil, err
		}
		return func(m map[string]interface{}) bool { return left(m) && right(m) }, nil

	case *sqlparser.OrExpr:
		left, err := buildMapPredicate(e.Left)
		if err != nil {
			return nil, err
		}
		right, err := buildMapPredicate(e.Right)
		if err != nil {
			return nil, err
		}
		return func(m map[string]interface{}) bool { return left(m) || right(m) }, nil

	case *sqlparser.ComparisonExpr:
		return buildComparisonPredicate(e)

	default:
		return nil, fmt.Errorf("unsupported WHERE expression type: %T", expr)
	}
}

// buildComparisonPredicate handles a direct comparison (e.g., `id = 5`).
func buildComparisonPredicate(expr *sqlparser.ComparisonExpr) (func(map[string]interface{}) bool, error) {
	col, ok := expr.Left.(*sqlparser.ColName)
	if !ok {
		return nil, fmt.Errorf("left side of comparison must be a column name, got %T", expr.Left)
	}
	fieldName := col.Name.String()

	lit, ok := expr.Right.(*sqlparser.ComparisonExpr)
	if !ok {
		return nil, fmt.Errorf("right side of comparison must be a literal value, got %T", expr.Right)
	}

	return func(objMap map[string]interface{}) bool {
		fieldValue, ok := objMap[fieldName]
		if !ok {
			return false // Field does not exist in the object map.
		}
		return compare(fieldValue, lit, expr.Operator)
	}, nil
}

// compare performs a type-aware comparison between a field value and a SQL literal.
func compare(fieldValue interface{}, literal *sqlparser.ComparisonExpr, op sqlparser.ComparisonExprOperator) bool {
	// JSON unmarshaling uses float64 for all numbers, so we handle that as the primary numeric type.
	if f64, ok := fieldValue.(float64); ok {
		litStr := sqlparser.String(literal)
		litVal, err := strconv.ParseFloat(litStr, 64)
		if err != nil {
			return false
		}
		switch op {
		case sqlparser.EqualOp:
			return f64 == litVal
		case sqlparser.NotEqualOp:
			return f64 != litVal
		case sqlparser.LessThanOp:
			return f64 < litVal
		case sqlparser.LessEqualOp:
			return f64 <= litVal
		case sqlparser.GreaterThanOp:
			return f64 > litVal
		case sqlparser.GreaterEqualOp:
			return f64 >= litVal
		default:
			panic("unhandled default case")
		}
	} else if str, ok := fieldValue.(string); ok {
		litStr := sqlparser.String(literal)
		switch op {
		case sqlparser.EqualOp:
			return str == litStr
		case sqlparser.NotEqualOp:
			return str != litStr
		case sqlparser.LessThanOp:
			return str < litStr
		case sqlparser.LessEqualOp:
			return str <= litStr
		case sqlparser.GreaterThanOp:
			return str > litStr
		case sqlparser.GreaterEqualOp:
			return str >= litStr
		default:
			panic("unhandled default case")
		}
	} else if b, ok := fieldValue.(bool); ok {
		litStr := sqlparser.String(literal)
		value, err := strconv.ParseBool(litStr)
		if err != nil {
			return false
		}
		switch op {
		case sqlparser.EqualOp:
			return b == value
		case sqlparser.NotEqualOp:
			return b != value
		default:
			panic("unhandled default case")
		}
	}

	return false
}

// objectToMap converts any struct into a map[string]interface{} using JSON marshaling.
func objectToMap[T any](obj T) (map[string]interface{}, error) {
	var data T
	jsonValue, err := sonic.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal data to JSON: %w", err)
	}

	var objectStructureMap map[string]interface{}
	err = sonic.Unmarshal(jsonValue, &objectStructureMap)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal JSON to map: %w", err)
	}

	return objectStructureMap, nil
}
