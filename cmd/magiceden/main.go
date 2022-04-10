package magiceden

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/fatih/color"
	"github.com/spf13/cobra"
	"github.com/valyala/fasthttp"
	"go.uber.org/ratelimit"

	_ "github.com/lib/pq"
)

var Cmd = &cobra.Command{
	Use:     "magiceden",
	Example: "echo https://api-mainnet.magiceden.dev/v2/collections > LIMIT=500 ./nftkitten magiceden >.out.json; [ $? -eq 0 ] && mv .out.json out.json  || rm .out.json",
	Run: func(cmd *cobra.Command, args []string) {
		limit := lookupEnvToI("LIMIT", 0)
		if rate := lookupEnvToI("RATE", 2); rate <= 0 {
			execute(limit, 2)
		} else {
			execute(limit, rate)
		}
	},
}

func lookupEnvToI(key string, defVal int) int {
	if env, _ := os.LookupEnv(key); env == "" {
		return defVal
	} else if envToI, err := strconv.Atoi(env); err != nil {
		return defVal
	} else {
		return envToI
	}
}

func execute(limit int, rate int) {
	log.Println(fmt.Sprint("LIMIT ", limit))
	limiter := ratelimit.New(rate)
	sep := ""
	scanner := bufio.NewScanner(os.Stdin)

	if limit > 0 {
		for scanner.Scan() {
			endpoint := strings.TrimSpace(os.ExpandEnv(scanner.Text()))
			if endpoint == "" {
				continue
			}
			if strings.Contains(endpoint, "?") {
				endpoint += "&"
			} else {
				endpoint += "?"
			}
			fetchMany(endpoint, &sep, limiter, limit)
		}
	} else {
		for scanner.Scan() {
			endpoint := strings.TrimSpace(os.ExpandEnv(scanner.Text()))
			if endpoint == "" {
				continue
			}

			limiter.Take()

			if res, err := sendRequest(endpoint); err != nil {
				color.New(color.FgHiMagenta).Fprintln(os.Stderr, err.Error())
			} else {
				printRow(res, &sep)
			}
		}
	}

	if err := scanner.Err(); err != nil {
		panic(err)
	}

	log.Println("done")
}

func printRow(row interface{}, sep *string) {
	if out, err := json.Marshal(row); err != nil {
		panic(err)
	} else {
		fmt.Print(*sep)
		fmt.Print(string(out))
		*sep = "\n"
	}
}

func sendRequest(url string) (interface{}, error) {
	log.Println(url)
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	req.Header.SetMethod("GET")
	req.Header.Set("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:99.0) Gecko/20100101 Firefox/99.0")
	req.Header.Set("Accept", "application/json, text/plain, */*")
	req.Header.Set("Accept-Language", "en-GB,en;q=0.5")
	req.Header.Set("Accept-Encoding", "gzip")
	req.Header.Set("Referer", "https://magiceden.io/")
	req.Header.Set("Origin", "https://magiceden.io")
	req.Header.Set("DNT", "1")
	req.Header.Set("Connection", "keep-alive")
	req.Header.Set("Sec-Fetch-Dest", "empty")
	req.Header.Set("Sec-Fetch-Mode", "cors")
	req.Header.Set("Sec-Fetch-Site", "same-site")
	req.Header.Set("Pragma", "no-cache")
	req.Header.Set("Cache-Control", "no-cache")
	req.Header.Set("TE", "trailers")
	req.SetRequestURI(url)
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)
	if err := fasthttp.Do(req, resp); err != nil {
		return nil, err
	} else {
		contentEncoding := resp.Header.Peek("Content-Encoding")
		if !bytes.EqualFold(contentEncoding, []byte("gzip")) {
			body := resp.Body()
			var output interface{}
			json.Unmarshal(body, &output)
			return output, nil
		} else if body, err := resp.BodyGunzip(); err != nil {
			return nil, err
		} else {
			var output interface{}
			json.Unmarshal(body, &output)
			return output, nil
		}
	}
}

func fetchMany(endpoint string, sep *string, limiter ratelimit.Limiter, limit int) {
	limiter.Take()
	if res, err := sendRequest(fmt.Sprint(endpoint, "limit=", limit)); err != nil {
		panic(err)
	} else if data, ok := res.([]interface{}); !ok {
		panic("Response is not array")
	} else if size := len(data); size > 0 {
		for _, row := range data {
			printRow(row, sep)
		}
		if size >= limit {
			fetchManyRecursive(endpoint, limiter, size, limit)
		}
	}
}

func fetchManyRecursive(endpoint string, limiter ratelimit.Limiter, offset int, limit int) {
	limiter.Take()
	if res, err := sendRequest(fmt.Sprint(endpoint, "limit=", limit, "&offset=", offset)); err != nil {
		panic(err)
	} else if data, ok := res.([]interface{}); !ok {
		panic("Response is not array")
	} else if size := len(data); size > 0 {
		sep := ""
		for _, row := range data {
			printRow(row, &sep)
		}
		if size >= limit {
			fetchManyRecursive(endpoint, limiter, offset+size, limit)
		}
	}
}
