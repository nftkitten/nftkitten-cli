package me

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/spf13/cobra"

	_ "github.com/lib/pq"
)

type Item struct {
	V interface{}
	E error
}

var db *sql.DB
var API_BASE_URL string
var SOLSCAN_PUBLIC_API_BASE_URL string

var Cmd = &cobra.Command{
	Use:   "me",
	Short: "crawl ME API",
	Run: func(cmd *cobra.Command, args []string) {
		full, _ := cmd.Flags().GetBool("full")
		var ok bool
		API_BASE_URL, ok = os.LookupEnv("API_BASE_URL")
		if !ok {
			log.Fatalln("No API_BASE_URL")
		}
		SOLSCAN_PUBLIC_API_BASE_URL, ok = os.LookupEnv("SOLSCAN_PUBLIC_API_BASE_URL")
		if !ok {
			log.Fatalln("No PUBLIC_SOLSCAN_API_BASE_URL")
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
		execute(full)
	},
}

func init() {
	Cmd.Flags().Bool("full", false, "Rescan new content")
}

func execute(full bool) {
	log.Println(API_BASE_URL)
	log.Println(SOLSCAN_PUBLIC_API_BASE_URL)
	log.Println(`initialize lookups`)

	log.Println(`initialize streams`)
	var wg sync.WaitGroup

	subscribeLaunchpad := subscribeCollection("launchpad", wg)
	subscribeCollection := subscribeCollection("collection", wg)

	log.Println(`produce events`)
	pageLimit := 10
	if full {
		pageLimit = UNLIMIT_PAGE
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for val := range fetchMany("launchpad/collections", 500, pageLimit, wg) {
			if val.E != nil {
				logError(val.E)
			} else {
				subscribeLaunchpad(val.V)
			}
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for val := range fetchMany("collections", 500, pageLimit, wg) {
			if val.E != nil {
				logError(val.E)
			} else {
				subscribeCollection(val.V)
			}
		}
	}()

	log.Println("wait.")
	wg.Wait()
	log.Println("reload completed.")
}
