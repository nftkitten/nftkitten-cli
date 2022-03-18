package me

import (
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/reactivex/rxgo/v2"
)

func subscribeWallet(
	pool *[]rxgo.Disposed,
	db *sql.DB,
	url string,
	filter func(string) bool,
) func(item interface{}) {
	return func(item interface{}) {
		address := item.(string)
		w := map[string]interface{}{"wallet_id": address}
		bytes, _ := json.Marshal(w)
		scanId := fmt.Sprint("wallet.", address)
		var ob rxgo.Observable
		if filter(scanId) {
			ob = dbExecuteMany(
				db,
				sqlForUpsert("wallet", "", address, bytes),
				sqlForUpsertScanLog(scanId),
			)
		} else {
			ob = rxgo.Just(item)()
		}
		*pool = append(*pool, ob.
			ForEach(func(i interface{}) {
				*pool = append(*pool, fetchMany(url, fmt.Sprint("wallets/", address, "/tokens"), 500).
					ForEach(subscribeWalletTokens(pool, address, db, filter), logError, doNothing, rxgo.WithCPUPool()))
				*pool = append(*pool, fetchMany(
					url,
					fmt.Sprint("wallets/", address, "/activities"),
					500,
				).
					ForEach(subscribeWalletActivites(pool, address, db, filter), logError, doNothing, rxgo.WithCPUPool()))
				*pool = append(*pool, fetchMany(url, fmt.Sprint("wallets/", address, "/offers_made"), 500).
					ForEach(subscribeWalletOffersMade(pool, address, db, filter), logError, doNothing, rxgo.WithCPUPool()))
				*pool = append(*pool, fetchMany(url, fmt.Sprint("wallets/", address, "/offers_received"), 500).
					ForEach(subscribeWalletOffersReceived(pool, address, db, filter), logError, doNothing, rxgo.WithCPUPool()))
				*pool = append(*pool, fetchOne(url, fmt.Sprint("wallets/", address, "/escrow_balance")).
					ForEach(subscribeWalletEscrowBalance(pool, address, db, filter), logError, doNothing, rxgo.WithCPUPool()))
			}, logError, doNothing, rxgo.WithCPUPool()))
	}
}

func subscribeWalletTokens(
	pool *[]rxgo.Disposed,
	address string,
	db *sql.DB,
	filter func(string) bool,
) func(interface{}) {
	return func(item interface{}) {
		m := item.(map[string]interface{})
		bytes, _ := json.Marshal(item)
		mintAddress := fmt.Sprint(m["mintAddress"])
		scanId := fmt.Sprint("wallet_token.", mintAddress)
		if filter(scanId) {
			*pool = append(*pool, dbExecuteMany(
				db,
				sqlForUpsertWithParent(
					"wallet",
					"token",
					address,
					bytes,
					mintAddress,
				),
				sqlForUpsertScanLog(scanId),
			).ForEach(doNothingOnNext, logError, doNothing, rxgo.WithCPUPool()))
		}
	}
}

func subscribeWalletActivites(
	pool *[]rxgo.Disposed,
	address string,
	db *sql.DB,
	filter func(string) bool,
) func(interface{}) {
	return func(item interface{}) {
		m := item.(map[string]interface{})
		bytes, _ := json.Marshal(item)
		signature := fmt.Sprint(m["signature"])
		scanId := fmt.Sprint("wallet_activity.", address, ".", signature)
		if filter(scanId) {
			*pool = append(*pool, dbExecuteMany(
				db,
				sqlForUpsertWithParent(
					"wallet",
					"activity",
					address, bytes, signature,
				),
				sqlForUpsertScanLog(scanId),
			).ForEach(doNothingOnNext, logError, doNothing, rxgo.WithCPUPool()))
		}
	}
}

func subscribeWalletOffersMade(
	pool *[]rxgo.Disposed,
	address string,
	db *sql.DB,
	filter func(string) bool,
) func(interface{}) {
	return func(item interface{}) {
		m := item.(map[string]interface{})
		bytes, _ := json.Marshal(item)
		pdaAddress := fmt.Sprint(m["pdaAddress"])
		scanId := fmt.Sprint("wallet_offers_made.", address, ".", pdaAddress)
		if filter(scanId) {
			*pool = append(*pool, dbExecuteMany(
				db,
				sqlForUpsertWithParent(
					"wallet",
					"offers_made",
					address, bytes, pdaAddress,
				),
				sqlForUpsertScanLog(scanId),
			).ForEach(doNothingOnNext, logError, doNothing, rxgo.WithCPUPool()))
		}
	}
}

func subscribeWalletOffersReceived(
	pool *[]rxgo.Disposed,
	address string,
	db *sql.DB,
	filter func(string) bool,
) func(interface{}) {
	return func(item interface{}) {
		m := item.(map[string]interface{})
		bytes, _ := json.Marshal(item)
		pdaAddress := fmt.Sprint(m["pdaAddress"])
		scanId := fmt.Sprint("wallet_offers_received.", address, ".", pdaAddress)
		if filter(scanId) {
			*pool = append(*pool, dbExecuteMany(
				db,
				sqlForUpsertWithParent(
					"wallet",
					"offers_received",ssszz
					address, bytes, pdaAddress,
				),
				sqlForUpsertScanLog(scanId),
			).ForEach(doNothingOnNext, logError, doNothing, rxgo.WithCPUPool()))
		}
	}
}

func subscribeWalletEscrowBalance(
	pool *[]rxgo.Disposed,
	address string,
	db *sql.DB,
	filter func(string) bool,
) func(interface{}) {
	return func(item interface{}) {
		bytes, _ := json.Marshal(item)
		scanId := fmt.Sprint("wallet_escrow_balance.", address, ".", address)
		if filter(scanId) {
			*pool = append(*pool, dbExecuteMany(
				db,
				sqlForUpsert("wallet_escrow_balance", "wallet_", address, bytes),
				sqlForUpsertScanLog(scanId),
			).ForEach(doNothingOnNext, logError, doNothing, rxgo.WithCPUPool()))
		}
	}
}
