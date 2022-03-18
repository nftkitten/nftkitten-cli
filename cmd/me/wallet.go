package me

// import (
// 	"database/sql"
// 	"encoding/json"
// 	"fmt"

// 	"github.com/reactivex/rxgo/v2"
// )

// func subscribeWalletTokens(
// 	pool *[]rxgo.Disposed,
// 	address string,
// 	db *sql.DB,
// ) func(interface{}) {
// 	return func(item interface{}) {
// 		m := item.(map[string]interface{})
// 		bytes, _ := json.Marshal(item)
// 		mintAddress := fmt.Sprint(m["mintAddress"])
// 		*pool = append(*pool, dbExecuteMany(
// 			db,
// 			sqlForUpsertWithParent(
// 				"wallet",
// 				"token",
// 				address,
// 				bytes,
// 				mintAddress,
// 			),
// 			sqlForUpsertScanLog("wallet_token", mintAddress),
// 		).ForEach(doNothingOnNext, logError, doNothing, rxgo.WithCPUPool()))
// 	}
// }

// // func subscribeWalletActivites(
// // 	pool *[]rxgo.Disposed,
// // 	address string,
// // 	db *sql.DB,
// // ) func(interface{}) {
// // 	return func(item interface{}) {
// // 		m := item.(map[string]interface{})
// // 		bytes, _ := json.Marshal(item)
// // 		signature := fmt.Sprint(m["signature"])
// // 		*pool = append(*pool, dbExecuteMany(
// // 			db,
// // 			sqlForUpsertWithParent(
// // 				"wallet",
// // 				"activity",
// // 				address, bytes, signature,
// // 			),
// // 			sqlForUpsertScanLog("wallet_activity", fmt.Sprint(address, ".", signature)),
// // 		).ForEach(doNothingOnNext, logError, doNothing, rxgo.WithCPUPool()))
// // 	}
// // }

// // func subscribeWalletOffersMade(
// // 	pool *[]rxgo.Disposed,
// // 	address string,
// // 	db *sql.DB,
// // ) func(interface{}) {
// // 	return func(item interface{}) {
// // 		m := item.(map[string]interface{})
// // 		bytes, _ := json.Marshal(item)
// // 		pdaAddress := fmt.Sprint(m["pdaAddress"])
// // 		*pool = append(*pool, dbExecuteMany(
// // 			db,
// // 			sqlForUpsertWithParent(
// // 				"wallet",
// // 				"offers_made",
// // 				address, bytes, pdaAddress,
// // 			),
// // 			sqlForUpsertScanLog("wallet_offers_made", fmt.Sprint(address, ".", pdaAddress)),
// // 		).ForEach(doNothingOnNext, logError, doNothing, rxgo.WithCPUPool()))
// // 	}
// // }

// // func subscribeWalletOffersReceived(
// // 	pool *[]rxgo.Disposed,
// // 	address string,
// // 	db *sql.DB,
// // ) func(interface{}) {
// // 	return func(item interface{}) {
// // 		m := item.(map[string]interface{})
// // 		bytes, _ := json.Marshal(item)
// // 		pdaAddress := fmt.Sprint(m["pdaAddress"])
// // 		*pool = append(*pool, dbExecuteMany(
// // 			db,
// // 			sqlForUpsertWithParent(
// // 				"wallet",
// // 				"offers_received",
// // 				address, bytes, pdaAddress,
// // 			),
// // 			sqlForUpsertScanLog("wallet_offers_received", fmt.Sprint(address, ".", pdaAddress)),
// // 		).ForEach(doNothingOnNext, logError, doNothing, rxgo.WithCPUPool()))
// // 	}
// // }

// func subscribeWalletEscrowBalance(
// 	pool *[]rxgo.Disposed,
// 	address string,
// 	db *sql.DB,
// ) func(interface{}) {
// 	return func(item interface{}) {
// 		bytes, _ := json.Marshal(item)
// 		*pool = append(*pool, dbExecuteMany(
// 			db,
// 			sqlForUpsert("wallet_escrow_balance", "wallet_", address, bytes),
// 			sqlForUpsertScanLog("wallet_escrow_balance", fmt.Sprint(address, ".", address)),
// 		).ForEach(doNothingOnNext, logError, doNothing, rxgo.WithCPUPool()))
// 	}
// }

// func subscribeWallet(
// 	pool *[]rxgo.Disposed,
// 	db *sql.DB,
// 	url string,
// ) func(item interface{}) {
// 	return func(item interface{}) {
// 		address := item.(string)
// 		w := map[string]interface{}{"wallet_id": address}
// 		bytes, _ := json.Marshal(w)
// 		*pool = append(*pool, dbExecuteMany(
// 			db,
// 			sqlForUpsert("wallet", "", address, bytes),
// 			sqlForUpsertScanLog("wallet", address),
// 		).
// 			ForEach(func(i interface{}) {
// 				*pool = append(*pool, fetchMany(url, fmt.Sprint("wallets/", address, "/tokens"), 500).
// 					ForEach(subscribeWalletTokens(pool, address, db), logError, doNothing, rxgo.WithCPUPool()))
// 				// *pool = append(*pool, fetchMany(
// 				// 	url,
// 				// 	fmt.Sprint("wallets/", address, "/activities"),
// 				// 	500,
// 				// ).
// 				// 	ForEach(subscribeWalletActivites(pool, address, db), logError, doNothing, rxgo.WithCPUPool()))
// 				// *pool = append(*pool, fetchMany(url, fmt.Sprint("wallets/", address, "/offers_made"), 500).
// 				// 	ForEach(subscribeWalletOffersMade(pool, address, db), logError, doNothing, rxgo.WithCPUPool()))
// 				// *pool = append(*pool, fetchMany(url, fmt.Sprint("wallets/", address, "/offers_received"), 500).
// 				// 	ForEach(subscribeWalletOffersReceived(pool, address, db), logError, doNothing, rxgo.WithCPUPool()))
// 				*pool = append(*pool, fetchOne(url, fmt.Sprint("wallets/", address, "/escrow_balance")).
// 					ForEach(subscribeWalletEscrowBalance(pool, address, db), logError, doNothing, rxgo.WithCPUPool()))
// 			}, logError, doNothing, rxgo.WithCPUPool()))
// 	}
// }
