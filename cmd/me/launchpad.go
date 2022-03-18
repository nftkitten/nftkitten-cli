package me

import (
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/reactivex/rxgo/v2"
)

func subscribeLaunchpad(pool *[]rxgo.Disposed, db *sql.DB, filter func(string) bool) func(interface{}) {
	return func(item interface{}) {
		m := item.(map[string]interface{})
		bytes, _ := json.Marshal(item)
		symbol := fmt.Sprint(m["symbol"])
		scanId := fmt.Sprint("launchpad.", symbol)
		if filter(scanId) {
			*pool = append(*pool, dbExecuteMany(
				db,
				sqlForUpsert("launchpad", "", symbol, bytes),
				sqlForUpsertScanLog(scanId),
			).ForEach(doNothingOnNext, logError, doNothing, rxgo.WithCPUPool()))
		}
	}
}
