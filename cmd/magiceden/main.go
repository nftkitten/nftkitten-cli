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
	Example: "ENDPOINT=https://api-mainnet.magiceden.dev/v2/collections LIMIT=500 nftkitten magiceden >.out.json; [ $? -eq 0 ] && mv .out.json out.json  || rm .out.json",
	Run: func(cmd *cobra.Command, args []string) {
		if endpoint, _ := os.LookupEnv("ENDPOINT"); endpoint == "" {
			log.Fatalln("No ENDPOINT")
		} else {
			limit := lookupEnvToI("LIMIT", 0)
			if rate := lookupEnvToI("RATE", 2); rate <= 0 {
				execute(endpoint, limit, 2)
			} else {
				execute(endpoint, limit, rate)
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

func execute(endpoint string, limit int, rate int) {
	log.Println(fmt.Sprint("ENDPOINT ", endpoint))
	log.Println(fmt.Sprint("LIMIT ", limit))

	if limit > 0 {
		if strings.Contains(endpoint, "?") {
			endpoint += "&"
		} else {
			endpoint += "?"
		}
		limiter := ratelimit.New(rate)
		fmt.Print("[")
		fetchMany(endpoint, limiter, limit)
		fmt.Print("]")
	} else if res, err := fetchOne(endpoint); err != nil {
		color.New(color.FgHiMagenta).Fprintln(os.Stderr, err.Error())
	} else if out, err := json.Marshal(res); err != nil {
		panic(err)
	} else {
		fmt.Print(string(out))
	}

	log.Println("done")
}

func fetchOne(url string) (interface{}, error) {
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

func fetchMany(endpoint string, limiter ratelimit.Limiter, limit int) {
	if res, err := fetchOne(fmt.Sprint(endpoint, "limit=", limit)); err != nil {
		panic(err)
	} else if data, ok := res.([]interface{}); !ok {
		panic("Response is not array")
	} else if size := len(data); size <= 0 {
		return
	} else {
		if out, err := json.Marshal(data[0]); err != nil {
			panic(err)
		} else {
			fmt.Print(string(out))
		}
		for i := 1; i < size; i++ {
			fmt.Print(",")
			if out, err := json.Marshal(data[i]); err != nil {
				panic(err)
			} else {
				fmt.Print(string(out))
			}
		}
		if size >= limit {
			fetchManyRecursive(endpoint, limiter, size, limit)
		}
	}
}

func fetchManyRecursive(endpoint string, limiter ratelimit.Limiter, offset int, limit int) {
	limiter.Take()
	if res, err := fetchOne(fmt.Sprint(endpoint, "limit=", limit, "&offset=", offset)); err != nil {
		panic(err)
	} else if data, ok := res.([]interface{}); !ok {
		panic("Response is not array")
	} else if size := len(data); size <= 0 {
		return
	} else {
		for i := 0; i < size; i++ {
			fmt.Print(",")
			if out, err := json.Marshal(data[i]); err != nil {
				panic(err)
			} else {
				fmt.Print(string(out))
			}
		}
		if size >= limit {
			fetchManyRecursive(endpoint, limiter, offset+size, limit)
		}
	}
}
