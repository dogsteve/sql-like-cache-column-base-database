package data_query

import (
	"a-eighty/mem_cache/map_table"

	"vitess.io/vitess/go/vt/sqlparser"
)

func HandleDelete(databaseName string, deleteStm *sqlparser.Delete) error {
	tableName := deleteStm.TableExprs[0].(*sqlparser.AliasedTableExpr)
	tableNameString := tableName.TableNameString()
	table, err := map_table.GetTable(databaseName, tableNameString)
	if err != nil {
		return err
	}
	if where := deleteStm.Where; where != nil {
		predicate, err := BuildPredicateFromExpr[map[string]any](where.Expr)
		if err == nil {
			err := table.Delete(predicate)
			if err != nil {
				return err
			}
			return nil
		}
	} else {
		err := table.Delete(func(a map[string]any) bool {
			return true
		})
		if err != nil {
			return err
		}
		return nil
	}
	return nil
}
