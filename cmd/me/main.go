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
		connStr := fmt.Sprint(
			"postgres://",
			os.Getenv("PGUSER"),
			":",
			os.Getenv("PGPASSWORD"),
			"@",
			os.Getenv("PGHOST"),
			"/",
			os.Getenv("PGDATABASE"),
			"?sslmode=disable",
		)
		var err error
		db, err = sql.Open("postgres", connStr)
		if err != nil {
			log.Fatal(err)
		}
		execute(force)
		db.Close()
		db = nil
	},
}

func init() {
	Cmd.Flags().Bool("force", false, "Rescan new content")
}

func execute(force bool) {
	url := os.Getenv("API_BASE_URL")
	fmt.Println(url)

	fmt.Println(`initialize lookups`)
	scanned := dbQueryScanLog()

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

	fmt.Println(`initialize streams`)
	launchpadPub := make(chan rxgo.Item)
	collectionPub := make(chan rxgo.Item)
	// tokenMintsPub := make(chan rxgo.Item)
	// walletAddressesPub := make(chan rxgo.Item)

	fmt.Println(`observe streams`)
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
		ForEach(subscribeCollection(url, filterScanned(force, scanned)), logError, doNothing, rxgo.WithCPUPool()))

	fmt.Println(`produce events`)
	go func() {
		fetchMany(url, "launchpad/collections", 500).Send(launchpadPub)
		fetchMany(url, "collections", 500).Send(collectionPub)

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
