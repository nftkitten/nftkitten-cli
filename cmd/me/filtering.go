package me

import (
	"context"
	"fmt"
)

func distinctByValue(_ context.Context, i interface{}) (interface{}, error) {
	return i, nil
}

func filterCollection(
	force bool,
	scannedIn24Hours map[string]bool,
) func(interface{}) bool {
	return func(item interface{}) bool {
		c := item.(map[string]interface{})
		if !force {
			if _, ok := scannedIn24Hours[fmt.Sprint("collection_stat.", fmt.Sprint(c["symbol"]))]; ok {
				return true
			}
		}
		return true
	}
}

func filterScanned(
	force bool,
	scanned map[string]bool,
) func(string) bool {
	return func(id string) bool {
		if !force {
			if _, ok := scanned[id]; ok {
				return false
			}
		}
		return true
	}
}

func filterToken(
	force bool,
	scannedIn24Hours map[string]bool,
) func(interface{}) bool {
	return func(item interface{}) bool {
		id := fmt.Sprint(item)
		if !force {
			if _, ok := scannedIn24Hours[fmt.Sprint("token.", id)]; ok {
				return false
			}
		}
		return false
	}
}

func filterWallet(
	force bool,
	scannedIn24Hours map[string]bool,
) func(interface{}) bool {
	return func(item interface{}) bool {
		id := item.(string)
		if !force {
			if _, ok := scannedIn24Hours[fmt.Sprint("token.", id)]; ok {
				return false
			}
		}
		return true
	}
}
