package magiceden

import (
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
	Example: "ENDPOINTS=https://api-mainnet.magiceden.dev/v2/collections LIMIT=500 nftkitten magiceden >.out.json; [ $? -eq 0 ] && mv .out.json out.json  || rm .out.json",
	Run: func(cmd *cobra.Command, args []string) {
		if endpoints, _ := os.LookupEnv("ENDPOINTS"); endpoints == "" {
			log.Fatalln("No ENDPOINT")
		} else {
			limit := lookupEnvToI("LIMIT", 0)
			if rate := lookupEnvToI("RATE", 2); rate <= 0 {
				execute(strings.Split(endpoints, "\n"), limit, 2)
			} else {
				execute(strings.Split(endpoints, "\n"), limit, rate)
			}
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

func execute(endpoints []string, limit int, rate int) {
	log.Println(fmt.Sprint("ENDPOINT ", strings.Join(endpoints, "\n")))
	log.Println(fmt.Sprint("LIMIT ", limit))
	limiter := ratelimit.New(rate)
	fmt.Print("[")
	sep := ""

	if limit > 0 {
		for _, endpoint := range endpoints {
			if strings.Contains(endpoint, "?") {
				endpoint += "&"
			} else {
				endpoint += "?"
			}
			fetchMany(endpoint, &sep, limiter, limit)
		}
	} else {
		for _, endpoint := range endpoints {
			limiter.Take()

			if res, err := sendRequest(endpoint); err != nil {
				color.New(color.FgHiMagenta).Fprintln(os.Stderr, err.Error())
			} else if out, err := json.Marshal(res); err != nil {
				panic(err)
			} else {
				fmt.Print(sep)
				fmt.Print(string(out))
				sep = ","
			}
		}
	}

	fmt.Print("]")
	log.Println("done")
}

func sendRequest(url string) (interface{}, error) {
	log.Println(url)
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	req.Header.SetMethod("GET")
	req.SetRequestURI(url)
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)
	if err := fasthttp.Do(req, resp); err != nil {
		return nil, err
	} else {
		var output interface{}
		json.Unmarshal(resp.Body(), &output)
		return output, nil
	}
}

func fetchMany(endpoint string, sep *string, limiter ratelimit.Limiter, limit int) {
	if res, err := sendRequest(fmt.Sprint(endpoint, "limit=", limit)); err != nil {
		panic(err)
	} else if data, ok := res.([]interface{}); !ok {
		panic("Response is not array")
	} else if size := len(data); size <= 0 {
		return
	} else {
		for _, row := range data {
			if out, err := json.Marshal(row); err != nil {
				panic(err)
			} else {
				fmt.Print(*sep)
				fmt.Print(string(out))
				*sep = ","
			}
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
	} else if size := len(data); size <= 0 {
		return
	} else {
		for _, row := range data {
			if out, err := json.Marshal(row); err != nil {
				panic(err)
			} else {
				fmt.Print(",")
				fmt.Print(string(out))
			}
		}
		if size >= limit {
			fetchManyRecursive(endpoint, limiter, offset+size, limit)
		}
	}
}
