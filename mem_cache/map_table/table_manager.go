package map_table

import (
	map_data_structure "a-eighty/data_structure/map"
	"a-eighty/utils"
	"errors"
	"sync/atomic"
)

var (
	atomicDatabaseRegistry atomic.Pointer[map_data_structure.TTLMap[string, map_data_structure.TTLMap[string, DataTable]]]
)

func InitDataBase() {
	databaseWrapper := map_data_structure.NewTTLMap[string, map_data_structure.TTLMap[string, DataTable]]()
	atomicDatabaseRegistry.Store(databaseWrapper)
}

func CreateDatabase(databaseName string) error {
	databaseName = utils.GetDefaultDatabaseName(databaseName)
	database := map_data_structure.NewTTLMap[string, DataTable]()
	databaseWrapper := atomicDatabaseRegistry.Load()
	databaseWrapper.Set(databaseName, database, -1)
	return nil
}

func CreateTable(databaseName string, tableName string) error {
	databaseName = utils.GetDefaultDatabaseName(databaseName)
	if tableName == "" {
		return errors.New("table name is empty")
	}
	if database, ok := atomicDatabaseRegistry.Load().Get(databaseName); ok {
		if _, ok := database.Get(tableName); ok {
			return errors.New("table already exists")
		}
		table := NewDataTable(tableName)
		database.Set(tableName, table, -1)
		return nil
	} else {
		return errors.New("database not exists")
	}
}

func GetTable(databaseName string, tableName string) (*DataTable, error) {
	databaseName = utils.GetDefaultDatabaseName(databaseName)
	if database, ok := atomicDatabaseRegistry.Load().Get(databaseName); ok {
		if table, ok := database.Get(tableName); ok {
			return table, nil
		} else {
			return nil, errors.New("table not exists")
		}
	} else {
		return nil, errors.New("database not exists")
	}
}
