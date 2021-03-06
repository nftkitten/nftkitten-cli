package me

import (
	"encoding/json"
	"fmt"
	"sync"
)

func subscribeCollection(
	table string,
	wg sync.WaitGroup,
) func(item interface{}) {
	return func(item interface{}) {
		m := item.(map[string]interface{})
		symbol := fmt.Sprint(m["symbol"])

		statsCh := make(chan Item)
		wg.Add(1)
		go func() {
			defer wg.Done()
			statsCh <- fetchOne(fmt.Sprint("collections/", symbol, "/stats"))
			close(statsCh)
		}()

		listingCh := make(chan Item)
		wg.Add(1)
		go func() {
			defer wg.Done()
			listingCh <- fetchOne(fmt.Sprint("collections/", symbol, "/listings?offset=0&limit=20"))
			close(listingCh)
		}()

		activitiesCh := make(chan Item)
		wg.Add(1)
		go func() {
			defer wg.Done()
			activitiesCh <- fetchOne(fmt.Sprint("collections/", symbol, "/activities?offset=0&limit=500"))
			close(activitiesCh)
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			statsItem := <-statsCh
			if statsItem.E != nil {
				logError(statsItem.E)
				return
			}
			stats, ok := statsItem.V.(map[string]interface{})
			if !ok {
				logError(fmt.Errorf("statsItem.V is not map[string]interface{}"))
				stats = make(map[string]interface{}, 0)
			}

			listingItem := <-listingCh
			if listingItem.E != nil {
				logError(listingItem.E)
				return
			}

			listing, ok := listingItem.V.([]interface{})
			if !ok {
				logError(fmt.Errorf("listing is not []interface{}"))
			} else if len(listing) > 0 {
				if listing1, ok := listing[0].(map[string]interface{}); !ok {
					logError(fmt.Errorf("listing1 is not map[string]interface{}"))
				} else {
					tokenMint := listing1["tokenMint"]
					url := fmt.Sprint(SOLSCAN_PUBLIC_API_BASE_URL, "/account/", tokenMint)
					if val, err := fetchFromSolScanApi(url, nil); err != nil {
						logError(err)
					} else if val != nil {
						stats["meta"] = val
					}
				}
			}
			stats["listing"] = listing
			activitiesItem := <-activitiesCh
			if activitiesItem.E != nil {
				logError(activitiesItem.E)
				return
			}
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
		}()
	}
}
