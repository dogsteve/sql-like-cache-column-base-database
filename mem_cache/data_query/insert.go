package data_query

import (
	"a-eighty/mem_cache/map_table"
	"a-eighty/utils"
	"errors"
	"strings"
	"time"

	"vitess.io/vitess/go/vt/sqlparser"
)

func HandleInsert(databaseName string, insertStm *sqlparser.Insert) error {
	table, err := insertStm.Table.TableName()
	if err != nil {
		return err
	}
	if table.IsEmpty() {
		return errors.New("table name is empty")
	}
	dataTable, err := map_table.GetTable(databaseName, table.Name.String())
	if err != nil {
		return err
	}
	var ttl time.Duration
	ttl = -1
	dataMap := make(map[string]any)
	var columns []string
	for _, col := range insertStm.Columns {
		columns = append(columns, sqlparser.String(col))
	}
	if values, ok := insertStm.Rows.(sqlparser.Values); ok {
		for _, row := range values {
			for j, expr := range row {
				colName := columns[j]
				valueString := sqlparser.String(expr)
				if strings.ToUpper(colName) == "TTL" {
					if duration, convertErr := utils.ParseISO8601Duration(valueString); convertErr == nil {
						ttl = duration
					}
					continue
				}
				dataMap[colName] = valueString
			}
		}
	}
	return dataTable.Insert(dataMap, ttl)
}
