package me

import (
	"database/sql"
	"fmt"
	"log"
	"os"

	"github.com/spf13/cobra"

	_ "github.com/lib/pq"
	"github.com/reactivex/rxgo/v2"
)

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
		db, err := sql.Open("postgres", connStr)
		if err != nil {
			log.Fatal(err)
		}
		execute(db, force)
		db.Close()
	},
}

func init() {
	Cmd.Flags().Bool("force", false, "Rescan new content")
}

func execute(db *sql.DB, force bool) {
	url := os.Getenv("API_BASE_URL")
	fmt.Println(url)

	fmt.Println(`initialize lookups`)
	scanned := dbQueryScanLog(db)

	tokenSet := dbQueryIdSet(`SELECT DISTINCT id FROM me_token
UNION SELECT DISTINCT CAST(data->'mintAddress' AS text) AS id FROM me_wallet_token
UNION SELECT DISTINCT CAST(data->'tokenMint' AS text) AS id FROM me_collection_activity
UNION SELECT DISTINCT CAST(data->'tokenMint' AS text) AS id FROM me_wallet_offers_made
UNION SELECT DISTINCT CAST(data->'tokenMint' AS text) AS id FROM me_wallet_offers_received`, db)

	walletSet := dbQueryIdSet(`SELECT DISTINCT wallet_id AS id FROM me_wallet_token
UNION SELECT DISTINCT CAST(data->'buyer' AS text) AS id FROM me_collection_activity
UNION SELECT DISTINCT CAST(data->'buyer' AS text) AS id FROM me_wallet_activity
UNION SELECT DISTINCT CAST(data->'buyer' AS text) AS id FROM me_wallet_offers_made
UNION SELECT DISTINCT CAST(data->'buyer' AS text) AS id FROM me_wallet_offers_received
UNION SELECT DISTINCT CAST(data->'seller' AS text) AS id FROM me_token_listing
UNION SELECT DISTINCT CAST(data->'seller' AS text) AS id FROM me_wallet_activity
UNION SELECT DISTINCT CAST(data->'seller' AS text) AS id FROM me_token_offer_received
UNION SELECT DISTINCT CAST(data->'seller' AS text) AS id FROM me_token_activity`, db)

	fmt.Println(`initialize streams`)
	launchpadPub := make(chan rxgo.Item)
	collectionPub := make(chan rxgo.Item)
	tokenMintsPub := make(chan rxgo.Item)
	walletAddressesPub := make(chan rxgo.Item)
	var pool []rxgo.Disposed

	fmt.Println(`observe streams`)
	pool = append(pool, rxgo.FromChannel(launchpadPub).
		ForEach(subscribeLaunchpad(db), logError, doNothing))
	pool = append(pool, rxgo.FromChannel(walletAddressesPub).
		Filter(filterWallet(force, scanned)).
		Distinct(distinctByValue).
		ForEach(subscribeWallet(&pool, db, url), logError, doNothing))
	pool = append(pool, rxgo.FromChannel(tokenMintsPub).
		Filter(filterToken(force, scanned)).
		Distinct(distinctByValue).
		ForEach(subscribeToken(&pool, db, url, walletAddressesPub), logError, doNothing))
	pool = append(pool, rxgo.FromChannel(collectionPub).
		Filter(filterCollection(force, scanned)).
		ForEach(subscribeCollection(&pool, db, url, tokenMintsPub, walletAddressesPub), logError, doNothing))

	fmt.Println(`produce events`)
	go func() {
		fetchMany(url, "launchpad/collections", 500).Send(launchpadPub)
		fetchMany(url, "collections", 500).Send(collectionPub)

		for id := range tokenSet {
			tokenMintsPub <- rxgo.Item{V: id}
		}

		for id := range walletSet {
			walletAddressesPub <- rxgo.Item{V: id}
		}
	}()

	log.Println("wait.")

	for _, disposed := range pool {
		<-disposed
	}

	db.Close()
	log.Println("reload completed.")
}
