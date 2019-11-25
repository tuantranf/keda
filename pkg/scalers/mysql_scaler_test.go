package scalers

import (
	"testing"
)

var testMysqlResolvedEnv = map[string]string{
	"test": "test",
}

type parseMysqlMetadataTestData struct {
	metadata   map[string]string
	isError    bool
	authParams map[string]string
}

var testMysqlMetadata = []parseMysqlMetadataTestData{
	// nothing passed
	{map[string]string{}, true, map[string]string{}},
	// properly formed meta
	{map[string]string{"host": "host", "port": "3306", "user": "root", "password": "password", "database": "test", "query": "SELECT count(*) from users", "count": "10" }, false, map[string]string{}},
	// empty database
	{map[string]string{"host": "host", "port": "3306", "user": "root", "password": "password", "query": "SELECT count(*) from users", "count": "10"}, true, map[string]string{}},
	// empty query
	{map[string]string{"host": "host", "port": "3306", "user": "root", "password": "password", "database": "test", "count": "10"}, true, map[string]string{}},
	// empty count
	{map[string]string{"host": "host", "port": "3306", "user": "root", "password": "password", "database": "test", "query": "SELECT count(*) from users"}, true, map[string]string{}},
	// password is defined in the authParams
	{map[string]string{"host": "host", "port": "3306", "user": "root", "database": "test", "query": "SELECT count(*) from users", "count": "10"}, false, map[string]string{"password": ""}},
}

func TestMysqlParseMetadata(t *testing.T) {
	for _, testData := range testMysqlMetadata {
		_, err := parseMysqlMetadata(testData.metadata, testMysqlResolvedEnv, testData.authParams)
		if err != nil && !testData.isError {
			t.Error("Expected success but got error", err)
		}
		if testData.isError && err == nil {
			t.Error("Expected error but got success")
		}
	}
}
