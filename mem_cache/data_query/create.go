package data_query

import (
	"a-eighty/mem_cache/map_table"
	"errors"

	"vitess.io/vitess/go/vt/sqlparser"
)

func HandleCreateTable(databaseName string, createTableStm *sqlparser.CreateTable) error {
	tableName := createTableStm.Table
	if tableName.IsEmpty() {
		return errors.New("table name is empty")
	}
	columns := createTableStm.GetTableSpec().Columns
	columnsMap := make(map[string]bool)
	for _, col := range columns {
		columnsMap[col.Name.String()] = true
	}
	tableNameString := tableName.Name.String()
	return map_table.CreateTable(databaseName, tableNameString)
}
