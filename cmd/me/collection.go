package me

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/fatih/color"
)

func subscribeCollection(
	table string,
	wg sync.WaitGroup,
) func(item interface{}) {
	return func(item interface{}) {
		m := item.(map[string]interface{})
		symbol := fmt.Sprint(m["symbol"])

		statsCh := make(chan Item)
		go func() {
			defer close(statsCh)
			statsCh <- fetchOne(fmt.Sprint("collections/", symbol, "/stats"))
		}()

		listingCh := make(chan Item)
		go func() {
			defer close(listingCh)
			listingCh <- fetchOne(fmt.Sprint("collections/", symbol, "/listings?offset=0&limit=20"))
		}()

		activitiesCh := make(chan Item)
		go func() {
			defer close(activitiesCh)
			activitiesCh <- fetchOne(fmt.Sprint("collections/", symbol, "/activities?offset=0&limit=500"))
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			statsItem := <-statsCh
			if statsItem.E != nil {
				logError(statsItem.E)
			} else {
				stats, ok := statsItem.V.(map[string]interface{})
				if !ok {
					logError(fmt.Errorf("statsItem.V is not map[string]interface{}"))
					stats = make(map[string]interface{}, 0)
				}

				if listingItem := <-listingCh; listingItem.E != nil {
					logError(listingItem.E)
				} else {
					listing, ok := listingItem.V.([]interface{})
					if !ok {
						logError(fmt.Errorf("listing is not []interface{}"))
					} else if len(listing) > 0 {
						listing1, ok := listing[0].(map[string]interface{})
						if !ok {
							logError(fmt.Errorf("listing1 is not map[string]interface{}"))
						} else {
							tokenMint := listing1["tokenMint"]
							url := fmt.Sprintf(SOLSCAN_PUBLIC_API_BASE_URL, "/token/meta?tokenAddress=", tokenMint)
							color.New(color.FgHiCyan).Println(url)
							val, err := fetchFromApi(url, nil)
							if err != nil {
								logError(err)
							} else if val != nil {
								stats["meta"] = val
							}
						}
					}
					stats["listing"] = listing
					if activitiesItem := <-activitiesCh; activitiesItem.E != nil {
						logError(activitiesItem.E)
					} else {
						stats["activities"] = activitiesItem.V
						bytes, _ := json.Marshal(item)
						statsBytes, _ := json.Marshal(stats)

						wg.Add(1)
						go func() {
							defer wg.Done()
							_, err := dbExecuteMany(sqlForUpsert(table, "", symbol, bytes, statsBytes))
							if err != nil {
								logError(err)
							}
						}()
					}
				}
			}
		}()
	}
}
