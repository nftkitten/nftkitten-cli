package me

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/fatih/color"
	"github.com/valyala/fasthttp"
	"go.uber.org/ratelimit"
)

var httpRateLimit ratelimit.Limiter = ratelimit.New(2)        // per second
var solScanHttpRateLimit ratelimit.Limiter = ratelimit.New(5) // per second
var UNLIMIT_PAGE = 0

func httpFetch(url string, limit ratelimit.Limiter) (interface{}, error) {
	limit.Take()
	switch limit {
	case solScanHttpRateLimit:
		color.New(color.FgHiGreen).Fprintln(os.Stderr, url)
	default:
		color.New(color.FgHiCyan).Fprintln(os.Stderr, url)
	}
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	req.Header.SetMethod("GET")
	req.SetRequestURI(url)
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)
	if err := fasthttp.Do(req, resp); err != nil {
		return nil, err
	}
	var output interface{}
	json.Unmarshal(resp.Body(), &output)
	return output, nil
}

func fetchOne(endpoint string) Item {
	val, err := fetchFromMEApi(fmt.Sprint(API_BASE_URL, "/", endpoint), nil)
	return Item{V: val, E: err}
}

func fetchMany(endpoint string, batchSize int, maxPage int, wg sync.WaitGroup) chan Item {
	ch := make(chan Item)
	wg.Add(1)
	go fetchManyRecursive(endpoint, batchSize, 0, maxPage, wg, ch)
	return ch
}

func fetchManyRecursive(endpoint string, batchSize int, offset int, maxPage int, wg sync.WaitGroup, ch chan Item) {
	defer wg.Done()
	if maxPage != UNLIMIT_PAGE && (1+offset/batchSize) > maxPage {
		return
	}
	if batchSize != 0 {
		log.Println(fmt.Sprint("query: ", endpoint, " #", (1 + offset/batchSize)))
	} else {
		log.Println(fmt.Sprint("query: ", endpoint))
	}
	var res interface{}
	var err error
	if batchSize > 0 {
		res, err = fetchFromMEApi(
			fmt.Sprint(API_BASE_URL, "/", endpoint, "?offset=", offset, "&limit=", batchSize),
			nil,
		)
	} else {
		res, err = fetchFromMEApi(
			fmt.Sprint(API_BASE_URL, "/", endpoint),
			nil,
		)
	}
	if err != nil {
		ch <- Item{E: err}
		close(ch)
		return
	}
	data, ok := res.([]interface{})
	if !ok || len(data) <= 0 {
		close(ch)
		return
	}
	for _, d := range data {
		ch <- Item{V: d}
	}
	if len(data) >= batchSize {
		wg.Add(1)
		go fetchManyRecursive(endpoint, batchSize, offset+batchSize, maxPage, wg, ch)
	}
}

func fetchFromMEApi(url string, defVal interface{}) (interface{}, error) {
	return httpFetch(url, httpRateLimit)
}

func fetchFromSolScanApi(url string, defVal interface{}) (interface{}, error) {
	return httpFetch(url, solScanHttpRateLimit)
}
