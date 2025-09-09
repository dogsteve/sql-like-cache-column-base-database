package utils

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/bytedance/sonic"
	"github.com/cespare/xxhash/v2"
)

func GetDefaultDatabaseName(databaseName string) string {
	if databaseName == "" {
		databaseName = "default"
	}
	return databaseName
}

func HashObject(obj map[string]any) (string, error) {
	keys := make([]string, 0, len(obj))
	for k := range obj {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	orderedMap := make(map[string]any, len(obj))
	for _, k := range keys {
		orderedMap[k] = obj[k]
	}
	b, err := sonic.Marshal(orderedMap)
	if err != nil {
		return "", err
	}
	hash := sha256.Sum256(b)
	return fmt.Sprintf("%x", hash), nil
}

func HashObject_XXHash(obj map[string]any) (uint64, error) {
	keys := make([]string, 0, len(obj))
	for k := range obj {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	orderedMap := make(map[string]any, len(obj))
	for _, k := range keys {
		orderedMap[k] = obj[k]
	}

	b, err := json.Marshal(orderedMap)
	if err != nil {
		return 0, err
	}

	return xxhash.Sum64(b), nil
}
