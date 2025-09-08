package test

import (
	"a-eighty/mem_cache/data_query"
	"a-eighty/mem_cache/map_table"
	"fmt"
	"testing"
)

func TestMemCache(t *testing.T) {
	map_table.InitDataBase()
	map_table.CreateDatabase("test")
	map_table.CreateTable("test", "employees")
	//map_data_structure.StartGlobalCleaner()
	sqlSession := data_query.SqlSession{
		DatabaseName: "test",
	}
	sql := "INSERT INTO employees (id, name, department, hire_date) VALUES (1, 'John Doe', 'Engineering', '2023-01-15');"
	for i := 0; i < 100; i++ {
		sqlSession.ExecuteSQL(sql)
	}
	selectSql := "select * from employees"
	rs, _ := sqlSession.ExecuteSQL(selectSql)
	fmt.Println(rs)
}
