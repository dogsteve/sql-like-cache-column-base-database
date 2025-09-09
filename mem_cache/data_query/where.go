package data_query

import (
	"fmt"
	"strconv"

	"github.com/bytedance/sonic"
	"github.com/davecgh/go-spew/spew"
	"vitess.io/vitess/go/vt/sqlparser"
)

func BuildPredicateFromExpr[T any](expr sqlparser.Expr) (func(T) bool, error) {
	mapPredicate, err := buildMapPredicate(expr)
	if err != nil {
		return nil, err
	}

	return func(obj T) bool {
		objMap, err := objectToMap(obj)
		if err != nil {

			return false
		}
		return mapPredicate(objMap)
	}, nil
}

func buildMapPredicate(expr sqlparser.Expr) (func(map[string]interface{}) bool, error) {
	switch expression := expr.(type) {
	case *sqlparser.AndExpr:
		left, err := buildMapPredicate(expression.Left)
		if err != nil {
			return nil, err
		}
		right, err := buildMapPredicate(expression.Right)
		if err != nil {
			return nil, err
		}
		return func(m map[string]interface{}) bool { return left(m) && right(m) }, nil

	case *sqlparser.OrExpr:
		left, err := buildMapPredicate(expression.Left)
		if err != nil {
			return nil, err
		}
		right, err := buildMapPredicate(expression.Right)
		if err != nil {
			return nil, err
		}
		return func(m map[string]interface{}) bool { return left(m) || right(m) }, nil

	case *sqlparser.ComparisonExpr:
		return buildComparisonPredicate(expression)

	default:
		return nil, fmt.Errorf("unsupported WHERE expression type: %T", expr)
	}
}

func buildComparisonPredicate(expr *sqlparser.ComparisonExpr) (func(map[string]interface{}) bool, error) {
	col, ok := expr.Left.(*sqlparser.ColName)
	if !ok {
		return nil, fmt.Errorf("left side of comparison must be a column name, got %T", expr.Left)
	}
	fieldName := col.Name.String()

	lit, ok := expr.Right.(*sqlparser.Literal)
	if !ok {
		return nil, fmt.Errorf("right side of comparison must be a literal value, got %T", expr.Right)
	}

	return func(objMap map[string]interface{}) bool {
		fieldValue, ok := objMap[fieldName]
		if !ok {
			return false
		}
		return compare(fieldValue, lit, expr.Operator)
	}, nil
}

func compare(fieldValue interface{}, literal *sqlparser.Literal, op sqlparser.ComparisonExprOperator) bool {
	stringFieldValue := spew.Sprintf("%v", fieldValue)
	if f64Val, err := strconv.ParseFloat(stringFieldValue, 64); err == nil {
		litStr := sqlparser.String(literal)
		litVal, err := strconv.ParseFloat(litStr, 64)
		if err != nil {
			return false
		}
		switch op {
		case sqlparser.EqualOp:
			return f64Val == litVal
		case sqlparser.NotEqualOp:
			return f64Val != litVal
		case sqlparser.LessThanOp:
			return f64Val < litVal
		case sqlparser.LessEqualOp:
			return f64Val <= litVal
		case sqlparser.GreaterThanOp:
			return f64Val > litVal
		case sqlparser.GreaterEqualOp:
			return f64Val >= litVal
		default:
			panic("unhandled default case")
		}
	} else if booleanVal, err := strconv.ParseBool(stringFieldValue); err == nil {
		litStr := sqlparser.String(literal)
		value, err := strconv.ParseBool(litStr)
		if err != nil {
			return false
		}
		switch op {
		case sqlparser.EqualOp:
			return booleanVal == value
		case sqlparser.NotEqualOp:
			return booleanVal != value
		default:
			panic("unhandled default case")
		}
	} else if strVal, ok := fieldValue.(string); ok {
		litStr := sqlparser.String(literal)
		switch op {
		case sqlparser.EqualOp:
			return strVal == litStr
		case sqlparser.NotEqualOp:
			return strVal != litStr
		case sqlparser.LessThanOp:
			return strVal < litStr
		case sqlparser.LessEqualOp:
			return strVal <= litStr
		case sqlparser.GreaterThanOp:
			return strVal > litStr
		case sqlparser.GreaterEqualOp:
			return strVal >= litStr
		default:
			panic("unhandled default case")
		}
	}
	return false
}

func objectToMap(obj any) (map[string]interface{}, error) {
	jsonValue, err := sonic.Marshal(obj)
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
