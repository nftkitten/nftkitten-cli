package me

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/fatih/color"
	"github.com/reactivex/rxgo/v2"
	"github.com/valyala/fasthttp"
	"go.uber.org/ratelimit"
)

var httpRateLimit ratelimit.Limiter = ratelimit.New(2) // per second

func httpFetch(url string) (interface{}, error) {
	httpRateLimit.Take()
	color.New(color.FgHiCyan).Println(url)
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

func fetchOne(url string, endpoint string) rxgo.Observable {
	pub := make(chan rxgo.Item)
	go func() {
		val, err := fetchFromApi(fmt.Sprint(url, "/", endpoint), nil)
		pub <- rxgo.Item{V: val, E: err}
		close(pub)
	}()
	return rxgo.FromChannel(pub)
}

func fetchMany(url string, endpoint string, batchSize int) rxgo.Observable {
	pub := make(chan rxgo.Item)
	go fetchManyRecursive(url, endpoint, batchSize, 0, pub)
	return rxgo.FromChannel(pub)
}

func fetchManyRecursive(url string, endpoint string, batchSize int, offset int, pub chan rxgo.Item) {
	if batchSize != 0 {
		log.Println(fmt.Sprint("query: ", endpoint, " #", (1 + offset/batchSize)))
	} else {
		log.Println(fmt.Sprint("query: ", endpoint))
	}
	var res interface{}
	var err error
	if batchSize > 0 {
		res, err = fetchFromApi(
			fmt.Sprint(url, "/", endpoint, "?offset=", offset, "&limit=", batchSize),
			nil,
		)
	} else {
		res, err = fetchFromApi(
			fmt.Sprint(url, "/", endpoint),
			nil,
		)
	}
	if err != nil {
		pub <- rxgo.Item{E: err}
		close(pub)
		return
	}
	data, ok := res.([]interface{})
	if !ok || len(data) <= 0 {
		close(pub)
		return
	}
	for _, d := range data {
		pub <- rxgo.Item{V: d}
	}
	fetchManyRecursive(url, endpoint, batchSize, offset+batchSize, pub)
}

func fetchFromApi(url string, defVal interface{}) (interface{}, error) {
	pub := make(chan rxgo.Item)
	go func() {
		rows, err := httpFetch(url)
		pub <- rxgo.Item{V: rows, E: err}
		close(pub)
	}()
	val := <-pub
	if val.E != nil {
		return nil, val.E
	}
	return val.V, nil
}
