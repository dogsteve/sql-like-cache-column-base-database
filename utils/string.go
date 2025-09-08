package utils

func GetDefaultDatabaseName(databaseName string) string {
	if databaseName == "" {
		databaseName = "default"
	}
	return databaseName
}
