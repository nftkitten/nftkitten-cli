package me

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/reactivex/rxgo/v2"
)

func subscribeToken(
	pool *[]rxgo.Disposed,
	db *sql.DB,
	url string,
	// walletAddressesPub chan rxgo.Item,
	filter func(string) bool,
) func(item interface{}) {
	return func(item interface{}) {
		mintAddress := item.(string)

		var ob rxgo.Observable
		scanId := fmt.Sprint("token.", mintAddress)
		if filter(scanId) {
			ob = fetchOne(url, fmt.Sprint("tokens/", mintAddress)).Map(func(_ context.Context, item interface{}) (interface{}, error) {
				bytes, _ := json.Marshal(item)
				*pool = append(*pool, dbExecuteMany(
					db,
					sqlForUpsert("token", "", mintAddress, bytes),
					sqlForUpsertScanLog(scanId),
				).ForEach(doNothingOnNext, logError, doNothing))
				return item, nil
			}, rxgo.WithCPUPool())
		} else {
			ob = rxgo.Just(item)()
		}
		*pool = append(*pool, ob.
			ForEach(func(i interface{}) {
				*pool = append(*pool, fetchMany(url, fmt.Sprint("tokens/", mintAddress, "/listings"), 0).
					ForEach(subscribeTokenListing(pool, mintAddress, db, filter), logError, doNothing, rxgo.WithCPUPool()))
				*pool = append(*pool, fetchMany(
					url,
					fmt.Sprint("tokens/", mintAddress, "/offer_received"),
					500,
				).
					ForEach(subscribeTokenOfferReceived(pool, mintAddress, db, filter), logError, doNothing, rxgo.WithCPUPool()))
				*pool = append(*pool, fetchMany(
					url,
					fmt.Sprint("tokens/", mintAddress, "/activities"),
					500,
				).
					ForEach(subscribeTokenActivites(pool, mintAddress, db, filter), logError, doNothing, rxgo.WithCPUPool()))
			}, logError, doNothing, rxgo.WithCPUPool()))
	}
}

func subscribeTokenListing(
	pool *[]rxgo.Disposed,
	mintAddress string,
	db *sql.DB,
	filter func(string) bool,
) func(interface{}) {
	return func(item interface{}) {
		m := item.(map[string]interface{})
		bytes, _ := json.Marshal(item)
		pdaAddress := fmt.Sprint(m["pdaAddress"])
		scanId := fmt.Sprint("token_listing.", mintAddress, ".", pdaAddress)
		if filter(scanId) {
			*pool = append(*pool, dbExecuteMany(
				db,
				sqlForUpsertWithParent(
					"token",
					"listing",
					mintAddress,
					bytes,
					pdaAddress,
				),
				sqlForUpsertScanLog(scanId),
			).ForEach(doNothingOnNext, logError, doNothing, rxgo.WithCPUPool()))
		}
	}
}

func subscribeTokenOfferReceived(
	pool *[]rxgo.Disposed,
	mintAddress string,
	db *sql.DB,
	filter func(string) bool,
) func(interface{}) {
	return func(item interface{}) {
		m := item.(map[string]interface{})
		bytes, _ := json.Marshal(item)
		pdaAddress := fmt.Sprint(m["pdaAddress"])
		scanId := fmt.Sprint("token_offer_received.", mintAddress, ".", pdaAddress)
		if filter(scanId) {
			*pool = append(*pool, dbExecuteMany(
				db,
				sqlForUpsertWithParent(
					"token",
					"offer_received",
					mintAddress,
					bytes,
					pdaAddress,
				),
				sqlForUpsertScanLog(scanId),
			).ForEach(doNothingOnNext, logError, doNothing, rxgo.WithCPUPool()))
		}
	}
}

func subscribeTokenActivites(
	pool *[]rxgo.Disposed,
	mintAddress string,
	db *sql.DB,
	filter func(string) bool,
) func(interface{}) {
	return func(item interface{}) {
		m := item.(map[string]interface{})
		bytes, _ := json.Marshal(item)
		signature := fmt.Sprint(m["signature"])
		scanId := fmt.Sprint("token_activity.", mintAddress, ".", signature)
		if filter(scanId) {
			*pool = append(*pool, dbExecuteMany(
				db,
				sqlForUpsertWithParent(
					"token",
					"activity",
					mintAddress,
					bytes,
					signature,
				),
				sqlForUpsertScanLog(scanId),
			).ForEach(doNothingOnNext, logError, doNothing, rxgo.WithCPUPool()))
		}
	}
}
