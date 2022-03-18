package me

import (
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/reactivex/rxgo/v2"
)

// func subscribeTokenListing(
// 	pool *[]rxgo.Disposed,
// 	mintAddress string,
// 	db *sql.DB,
// ) func(interface{}) {
// 	return func(item interface{}) {
// 		m := item.(map[string]interface{})
// 		bytes, _ := json.Marshal(item)
// 		pdaAddress := fmt.Sprint(m["pdaAddress"])
// 		*pool = append(*pool, dbExecuteMany(
// 			db,
// 			sqlForUpsertWithParent(
// 				"token",
// 				"listing",
// 				mintAddress,
// 				bytes,
// 				pdaAddress,
// 			),
// 			sqlForUpsertScanLog("token_listing", fmt.Sprint(mintAddress, ".", pdaAddress)),
// 		).ForEach(doNothingOnNext, logError, doNothing, rxgo.WithCPUPool()))
// 	}
// }

// func subscribeTokenOfferReceived(
// 	pool *[]rxgo.Disposed,
// 	mintAddress string,
// 	db *sql.DB,
// ) func(interface{}) {
// 	return func(item interface{}) {
// 		m := item.(map[string]interface{})
// 		bytes, _ := json.Marshal(item)
// 		pdaAddress := fmt.Sprint(m["pdaAddress"])
// 		*pool = append(*pool, dbExecuteMany(
// 			db,
// 			sqlForUpsertWithParent(
// 				"token",
// 				"offer_received",
// 				mintAddress,
// 				bytes,
// 				pdaAddress,
// 			),
// 			sqlForUpsertScanLog("token_offer_received", fmt.Sprint(mintAddress, ".", pdaAddress)),
// 		).ForEach(doNothingOnNext, logError, doNothing, rxgo.WithCPUPool()))
// 	}
// }

// func subscribeTokenActivites(
// 	pool *[]rxgo.Disposed,
// 	mintAddress string,
// 	db *sql.DB,
// ) func(interface{}) {
// 	return func(item interface{}) {
// 		m := item.(map[string]interface{})
// 		bytes, _ := json.Marshal(item)
// 		signature := fmt.Sprint(m["signature"])
// 		*pool = append(*pool, dbExecuteMany(
// 			db,
// 			sqlForUpsertWithParent(
// 				"token",
// 				"activity",
// 				mintAddress,
// 				bytes,
// 				signature,
// 			),
// 			sqlForUpsertScanLog("token_activity", fmt.Sprint(mintAddress, ".", signature)),
// 		).ForEach(doNothingOnNext, logError, doNothing, rxgo.WithCPUPool()))
// 	}
// }

func subscribeToken(
	pool *[]rxgo.Disposed,
	db *sql.DB,
	url string,
	// walletAddressesPub chan rxgo.Item,
) func(item interface{}) {
	return func(item interface{}) {
		mintAddress := item.(string)
		*pool = append(*pool, fetchOne(url, fmt.Sprint("tokens/", mintAddress)).
			ForEach(func(i interface{}) {
				bytes, _ := json.Marshal(i)
				<-dbExecuteMany(
					db,
					sqlForUpsert("token", "", mintAddress, bytes),
					sqlForUpsertScanLog("token", fmt.Sprint(mintAddress)),
				).
					ForEach(func(i interface{}) {
						// *pool = append(*pool, fetchMany(url, fmt.Sprint("tokens/", mintAddress, "/listings"), 0).
						// 	ForEach(subscribeTokenListing(pool, mintAddress, db), logError, doNothing, rxgo.WithCPUPool()))
						// *pool = append(*pool, fetchMany(
						// 	url,
						// 	fmt.Sprint("tokens/", mintAddress, "/offer_received"),
						// 	500,
						// ).
						// 	ForEach(subscribeTokenOfferReceived(pool, mintAddress, db), logError, doNothing, rxgo.WithCPUPool()))
						// *pool = append(*pool, fetchMany(
						// 	url,
						// 	fmt.Sprint("tokens/", mintAddress, "/activities"),
						// 	500,
						// ).
						// 	ForEach(subscribeTokenActivites(pool, mintAddress, db), logError, doNothing, rxgo.WithCPUPool()))
					}, logError, doNothing, rxgo.WithCPUPool())
			}, logError, doNothing, rxgo.WithCPUPool()))
	}
}
