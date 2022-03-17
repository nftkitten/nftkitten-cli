package me

import (
	"database/sql"
	"encoding/json"
	"fmt"
)

func subscribeLaunchpad(db *sql.DB) func(interface{}) {
	return func(item interface{}) {
		m := item.(map[string]interface{})
		bytes, _ := json.Marshal(item)
		symbol := fmt.Sprint(m["symbol"])
		<-dbExecuteMany(
			db,
			sqlForUpsert("launchpad", "", symbol, bytes),
			sqlForUpsertScanLog("launchpad", symbol),
		).ForEach(doNothingOnNext, logError, doNothing)
	}
}
