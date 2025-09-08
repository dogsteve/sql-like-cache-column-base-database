package test

import (
	"a-eighty/mem_cache/data_query"
	"testing"
)

func TestSql(t *testing.T) {
	databaseProvider := data_query.SqlSession{}
	databaseProvider.ExecuteSQL("delete from a where x = 1")
}
