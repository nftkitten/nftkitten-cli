package me

import (
	"encoding/json"
	"fmt"

	"github.com/reactivex/rxgo/v2"
)

func subscribeLaunchpad() func(interface{}) {
	return func(item interface{}) {
		m := item.(map[string]interface{})
		bytes, _ := json.Marshal(item)
		symbol := fmt.Sprint(m["symbol"])
		<-dbExecuteMany(
			sqlForUpsertLaunchpad("launchpad", "", symbol, bytes),
		).ForEach(doNothingOnNext, logError, doNothing, rxgo.WithCPUPool())
	}
}
