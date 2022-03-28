package me

import (
	"encoding/json"
	"fmt"
	"sync"
)

func subscribeCollection(
	table string,
	url string,
	wg sync.WaitGroup,
) func(item interface{}) {
	return func(item interface{}) {
		m := item.(map[string]interface{})
		symbol := fmt.Sprint(m["symbol"])

		statsCh := make(chan Item)
		go func() {
			defer close(statsCh)
			statsCh <- fetchOne(url, fmt.Sprint("collections/", symbol, "/stats"))
		}()

		listingCh := make(chan Item)
		go func() {
			defer close(listingCh)
			listingCh <- fetchOne(url, fmt.Sprint("collections/", symbol, "/listings?offset=0&limit=20"))
		}()

		activitiesCh := make(chan Item)
		go func() {
			defer close(activitiesCh)
			activitiesCh <- fetchOne(url, fmt.Sprint("collections/", symbol, "/activities?offset=0&limit=500"))
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
					stats = make(map[string]interface{}, 0)
				}

				if listingItem := <-listingCh; listingItem.E != nil {
					logError(listingItem.E)
				} else {
					stats["listing"] = listingItem.V
					if activitiesItem := <-activitiesCh; activitiesItem.E != nil {
						logError(activitiesItem.E)
					} else {
						stats["activities"] = activitiesItem.V
						bytes, _ := json.Marshal(item)
						statsBytes, _ := json.Marshal(stats)

						wg.Add(1)
						go func() {
							defer wg.Done()
							for item := range dbExecuteMany(sqlForUpsert(table, "", symbol, bytes, statsBytes)) {
								if item.E != nil {
									logError(item.E)
								}
							}
						}()
					}
				}
			}
		}()
	}
}
