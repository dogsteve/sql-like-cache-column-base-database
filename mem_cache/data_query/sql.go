package data_query

import (
	"errors"
	"fmt"

	"vitess.io/vitess/go/vt/sqlparser"
)

type QueryResult struct {
	Rows         []map[string]any
	RowsAffected uint64
}

// SQL Session
type SqlSession struct {
	DatabaseName string
}

func (sqlSession *SqlSession) ExecuteSQL(query string) (*QueryResult, error) {
	parser, err := sqlparser.New(sqlparser.Options{})
	if err != nil {
		return nil, err
	}
	stmt, err := parser.Parse(query)
	if err != nil {
		return nil, fmt.Errorf("failed to parse SQL query: %w", err)
	}

	switch s := stmt.(type) {
	case *sqlparser.Select:
		result, err := HandleSelect(sqlSession.DatabaseName, s)
		if err != nil {
			return nil, err
		}
		return &QueryResult{RowsAffected: uint64(len(result)), Rows: result}, nil
	case *sqlparser.Insert:
		err := HandleInsert(sqlSession.DatabaseName, s)
		if err != nil {
			return nil, err
		}
		return &QueryResult{RowsAffected: 1}, nil
	case *sqlparser.Delete:
		err := HandleDelete(sqlSession.DatabaseName, s)
		if err != nil {
			return nil, err
		}
	case *sqlparser.CreateTable:
		err := HandleCreateTable(sqlSession.DatabaseName, s)
		if err != nil {
			return nil, err
		}
		return &QueryResult{RowsAffected: 1}, nil
	case *sqlparser.CreateDatabase:
		{
			panic("")
		}
	default:
		return nil, fmt.Errorf("unsupported statement type: %T", stmt)
	}
	return nil, errors.New("not implement")
}
