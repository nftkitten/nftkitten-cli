package me

import (
	"encoding/json"
	"fmt"

	"github.com/reactivex/rxgo/v2"
)

func subscribeLaunchpad(filter func(string) bool) func(interface{}) {
	return func(item interface{}) {
		m := item.(map[string]interface{})
		bytes, _ := json.Marshal(item)
		symbol := fmt.Sprint(m["symbol"])
		scanId := fmt.Sprint("launchpad.", symbol)
		if filter(scanId) {
			pool = append(pool, dbExecuteMany(
				sqlForUpsert("launchpad", "", symbol, bytes),
				// sqlForUpsertScanLog(scanId),
			).ForEach(doNothingOnNext, logError, doNothing, rxgo.WithCPUPool()))
		}
	}
}
