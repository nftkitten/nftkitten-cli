package me

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/spf13/cobra"

	_ "github.com/lib/pq"
	"github.com/reactivex/rxgo/v2"
)

var wg sync.WaitGroup
var pool []rxgo.Disposed
var db *sql.DB

var Cmd = &cobra.Command{
	Use:   "me",
	Short: "crawl ME API",
	Run: func(cmd *cobra.Command, args []string) {
		force, _ := cmd.Flags().GetBool("force")
		API_BASE_URL, ok := os.LookupEnv("API_BASE_URL")
		if !ok {
			log.Fatalln("No API_BASE_URL")
		}
		PGUSER, ok := os.LookupEnv("PGUSER")
		if !ok {
			log.Fatalln("No PGUSER")
		}
		PGPASSWORD, ok := os.LookupEnv("PGPASSWORD")
		if !ok {
			log.Fatalln("No PGPASSWORD")
		}
		PGHOST, ok := os.LookupEnv("PGHOST")
		if !ok {
			log.Fatalln("No PGHOST")
		}
		PGDATABASE, ok := os.LookupEnv("PGDATABASE")
		if !ok {
			log.Fatalln("No PGDATABASE")
		}
		connStr := fmt.Sprint(
			"postgres://",
			PGUSER,
			":",
			PGPASSWORD,
			"@",
			PGHOST,
			"/",
			PGDATABASE,
			"?sslmode=require",
		)
		var err error
		db, err = sql.Open("postgres", connStr)
		if err != nil {
			log.Fatalln(err)
		}
		execute(API_BASE_URL, force)
		db.Close()
		db = nil
	},
}

func init() {
	Cmd.Flags().Bool("force", false, "Rescan new content")
}

func execute(apiBaseUrl string, force bool) {
	log.Println(apiBaseUrl)
	log.Println(`initialize lookups`)
	// scanned := dbQueryScanLog()
	scanned := make(map[string]bool)

	// tokenSet := dbQueryIdSet(`SELECT DISTINCT id FROM me_token
	// UNION SELECT DISTINCT CAST(data->'tokenMint' AS text) AS id FROM me_collection_listing
	// UNION SELECT DISTINCT CAST(data->'mintAddress' AS text) AS id FROM me_wallet_token
	// UNION SELECT DISTINCT CAST(data->'tokenMint' AS text) AS id FROM me_collection_activity
	// UNION SELECT DISTINCT CAST(data->'tokenMint' AS text) AS id FROM me_wallet_offers_made
	// UNION SELECT DISTINCT CAST(data->'tokenMint' AS text) AS id FROM me_wallet_offers_received`)
	// 	tokenSet := dbQueryIdSet(`SELECT DISTINCT id FROM me_token
	// UNION SELECT DISTINCT CAST(data->'tokenMint' AS text) AS id FROM me_collection_listing`)

	// walletSet := dbQueryIdSet(`SELECT DISTINCT wallet_id AS id FROM me_wallet_token
	// UNION SELECT DISTINCT CAST(data->'buyer' AS text) AS id FROM me_collection_activity
	// UNION SELECT DISTINCT CAST(data->'buyer' AS text) AS id FROM me_wallet_activity
	// UNION SELECT DISTINCT CAST(data->'buyer' AS text) AS id FROM me_wallet_offers_made
	// UNION SELECT DISTINCT CAST(data->'buyer' AS text) AS id FROM me_wallet_offers_received
	// UNION SELECT DISTINCT CAST(data->'seller' AS text) AS id FROM me_token_listing
	// UNION SELECT DISTINCT CAST(data->'seller' AS text) AS id FROM me_wallet_activity
	// UNION SELECT DISTINCT CAST(data->'seller' AS text) AS id FROM me_token_offer_received
	// UNION SELECT DISTINCT CAST(data->'seller' AS text) AS id FROM me_token_activity`)

	log.Println(`initialize streams`)
	launchpadPub := make(chan rxgo.Item)
	collectionPub := make(chan rxgo.Item)
	// tokenMintsPub := make(chan rxgo.Item)
	// walletAddressesPub := make(chan rxgo.Item)

	log.Println(`observe streams`)
	pool = append(pool, rxgo.FromChannel(launchpadPub).
		ForEach(subscribeLaunchpad(filterScanned(force, scanned)), logError, doNothing, rxgo.WithCPUPool()))
	// pool = append(pool, rxgo.FromChannel(walletAddressesPub).
	// 	Distinct(distinctByValue).
	// 	ForEach(subscribeWallet(url), logError, doNothing, rxgo.WithCPUPool()))
	// pool = append(pool, rxgo.FromChannel(tokenMintsPub).
	// 	Distinct(distinctByValue).
	// 	// ForEach(subscribeToken(url, walletAddressesPub), logError, doNothing, rxgo.WithCPUPool()))
	// 	ForEach(subscribeToken(url, filterScanned(force, scanned)), logError, doNothing, rxgo.WithCPUPool()))
	pool = append(pool, rxgo.FromChannel(collectionPub).
		// ForEach(subscribeCollection(url, tokenMintsPub, walletAddressesPub), logError, doNothing, rxgo.WithCPUPool()))
		ForEach(subscribeCollection(apiBaseUrl, filterScanned(force, scanned)), logError, doNothing, rxgo.WithCPUPool()))

	log.Println(`produce events`)
	go func() {
		fetchMany(apiBaseUrl, "launchpad/collections", 500).Send(launchpadPub)
		fetchMany(apiBaseUrl, "collections", 500).Send(collectionPub)

		// for id := range tokenSet {
		// 	tokenMintsPub <- rxgo.Item{V: id}
		// }

		// for id := range walletSet {
		// 	walletAddressesPub <- rxgo.Item{V: id}
		// }
	}()

	log.Println("wait.")

	for _, disposed := range pool {
		<-disposed
	}

	pool = nil
	log.Println("reload completed.")
}
