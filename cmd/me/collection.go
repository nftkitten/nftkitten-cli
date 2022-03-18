package me

import (
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/reactivex/rxgo/v2"
)

func subscribeCollectionListing(
	pool *[]rxgo.Disposed,
	symbol string,
	db *sql.DB,
	tokenMintsPub chan rxgo.Item,
	// walletAddressesPub chan rxgo.Item,
) func(interface{}) {
	return func(item interface{}) {
		m := item.(map[string]interface{})
		bytes, _ := json.Marshal(item)
		pdaAddress := fmt.Sprint(m["pdaAddress"])
		*pool = append(*pool, dbExecuteMany(
			db,
			sqlForUpsertWithParent("collection", "listing",
				symbol,
				bytes,
				pdaAddress,
			),
			sqlForUpsertScanLog("collection_listing", fmt.Sprint(symbol, ".", pdaAddress)),
		).ForEach(doNothingOnNext, logError, doNothing, rxgo.WithCPUPool()))
		if fmt.Sprint(m["tokenMint"]) != "" {
			tokenMintsPub <- rxgo.Item{V: fmt.Sprint(m["tokenMint"])}
		}
		// if fmt.Sprint(m["seller"]) != "" {
		// 	walletAddressesPub <- rxgo.Item{V: fmt.Sprint(m["seller"])}
		// }
	}
}

func subscribeCollectionActivity(
	pool *[]rxgo.Disposed,
	symbol string,
	db *sql.DB,
	tokenMintsPub chan rxgo.Item,
	// walletAddressesPub chan rxgo.Item,
) func(interface{}) {
	return func(item interface{}) {
		m := item.(map[string]interface{})
		bytes, _ := json.Marshal(item)
		signature := fmt.Sprint(m["signature"])
		*pool = append(*pool, dbExecuteMany(
			db,
			sqlForUpsertWithParent(
				"collection",
				"activity",
				symbol,
				bytes,
				signature,
			),
			sqlForUpsertScanLog("collection_activity", fmt.Sprint(symbol, ".", signature)),
		).ForEach(doNothingOnNext, logError, doNothing, rxgo.WithCPUPool()))
		if fmt.Sprint(m["tokenMint"]) != "" {
			tokenMintsPub <- rxgo.Item{V: fmt.Sprint(m["tokenMint"])}
		}
		// if fmt.Sprint(m["buyer"]) != "" {
		// 	walletAddressesPub <- rxgo.Item{V: fmt.Sprint(m["buyer"])}
		// }
		// if fmt.Sprint(m["seller"]) != "" {
		// 	walletAddressesPub <- rxgo.Item{V: fmt.Sprint(m["seller"])}
		// }
	}
}

func subscribeCollectionStat(
	pool *[]rxgo.Disposed,
	symbol string,
	db *sql.DB,
) func(interface{}) {
	return func(item interface{}) {
		bytes, _ := json.Marshal(item)
		*pool = append(*pool, dbExecuteMany(
			db,
			sqlForUpsert(
				"collection_stat",
				"collection_",
				symbol,
				bytes,
			),
			sqlForUpsertScanLog("collection_stat", symbol),
		).ForEach(doNothingOnNext, logError, doNothing, rxgo.WithCPUPool()))
	}
}

func subscribeCollection(
	pool *[]rxgo.Disposed,
	db *sql.DB,
	url string,
	tokenMintsPub chan rxgo.Item,
	// walletAddressesPub chan rxgo.Item,
) func(item interface{}) {
	return func(item interface{}) {
		m := item.(map[string]interface{})
		bytes, _ := json.Marshal(item)
		symbol := fmt.Sprint(m["symbol"])
		*pool = append(*pool, dbExecuteMany(
			db,
			sqlForUpsert("collection", "", symbol, bytes),
			sqlForUpsertScanLog("collection", symbol),
		).ForEach(doNothingOnNext, logError, doNothing, rxgo.WithCPUPool()))
		*pool = append(*pool, fetchMany(url, fmt.Sprint("collections/", symbol, "/listings"), 20).
			// ForEach(subscribeCollectionListing(pool, symbol, db, tokenMintsPub, walletAddressesPub), logError, doNothing, rxgo.WithCPUPool()))
			ForEach(subscribeCollectionListing(pool, symbol, db, tokenMintsPub), logError, doNothing, rxgo.WithCPUPool()))
		*pool = append(*pool, fetchMany(url, fmt.Sprint("collections/", symbol, "/activities"), 500).
			// ForEach(subscribeCollectionActivity(pool, symbol, db, tokenMintsPub, walletAddressesPub, rxgo.WithCPUPool()), logError, doNothing, rxgo.WithCPUPool()))
			ForEach(subscribeCollectionActivity(pool, symbol, db, tokenMintsPub), logError, doNothing, rxgo.WithCPUPool()))
		*pool = append(*pool, fetchOne(url, fmt.Sprint("collections/", symbol, "/stats")).
			ForEach(subscribeCollectionStat(pool, symbol, db), logError, doNothing, rxgo.WithCPUPool()))
	}
}
