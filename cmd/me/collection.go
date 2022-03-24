package me

import (
	"encoding/json"
	"fmt"

	"github.com/reactivex/rxgo/v2"
)

func subscribeCollection(
	url string,
	filter func(string) bool,
) func(item interface{}) {
	return func(item interface{}) {
		m := item.(map[string]interface{})
		stats := make(chan map[string]interface{})
		symbol := fmt.Sprint(m["symbol"])
		<-fetchOne(url, fmt.Sprint("collections/", symbol, "/stats")).ForEach(func(item interface{}) {
			stats <- item.(map[string]interface{})
		}, logError, doNothing)
		bytes, _ := json.Marshal(item)
		statsBytes, _ := json.Marshal(stats)
		pool = append(pool, dbExecuteMany(
			sqlForUpsertCollection("collection", "", symbol, bytes, statsBytes),
		).ForEach(doNothingOnNext, logError, doNothing, rxgo.WithCPUPool()))
	}
}
