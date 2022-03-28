package me

import (
	"encoding/json"
	"fmt"
	"log"

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
		color.New(color.FgHiGreen).Println(url)
	default:
		color.New(color.FgHiCyan).Println(url)
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

func fetchMany(endpoint string, batchSize int, maxPage int) chan Item {
	ch := make(chan Item)
	go fetchManyRecursive(endpoint, batchSize, 0, maxPage, ch)
	return ch
}

func fetchManyRecursive(endpoint string, batchSize int, offset int, maxPage int, ch chan Item) {
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
	fetchManyRecursive(endpoint, batchSize, offset+batchSize, maxPage, ch)
}

func fetchFromMEApi(url string, defVal interface{}) (interface{}, error) {
	pub := make(chan Item)
	go func() {
		rows, err := httpFetch(url, httpRateLimit)
		pub <- Item{V: rows, E: err}
		close(pub)
	}()
	val := <-pub
	if val.E != nil {
		return nil, val.E
	}
	return val.V, nil
}

func fetchFromSolScanApi(url string, defVal interface{}) (interface{}, error) {
	pub := make(chan Item)
	go func() {
		rows, err := httpFetch(url, solScanHttpRateLimit)
		pub <- Item{V: rows, E: err}
		close(pub)
	}()
	val := <-pub
	if val.E != nil {
		return nil, val.E
	}
	return val.V, nil
}
