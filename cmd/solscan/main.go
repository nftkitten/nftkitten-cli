package solscan

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
	Use:     "solscan",
	Example: "ENDPOINTS=https://api.solscan.io/collection?sortBy=volume LIMIT=500 nftkitten solscan >.out.json; [ $? -eq 0 ] && mv .out.json out.json  || rm .out.json",
	Run: func(cmd *cobra.Command, args []string) {
		if endpoints, _ := os.LookupEnv("ENDPOINTS"); endpoints == "" {
			log.Fatalln("No ENDPOINTS")
		} else {
			limit := lookupEnvToI("LIMIT", 0)
			if rate := lookupEnvToI("RATE", 5); rate <= 0 {
				execute(strings.Split(endpoints, "\n"), limit, 5)
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
	sep := ""
	fmt.Print("[")

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
			} else if rpn, ok := res.(map[string]interface{}); !ok {
				panic("Response is not object")
			} else if success, ok := rpn["success"].(bool); !ok || !success {
				panic("success is not true")
			} else if data, ok := rpn["data"].(interface{}); !ok || data == nil {
				panic("data is not found")
			} else if out, err := json.Marshal(data); err != nil {
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
	} else if rpn, ok := res.(map[string]interface{}); !ok {
		panic("Response is not object")
	} else if success, ok := rpn["success"].(bool); !ok || !success {
		panic("success is not true")
	} else if total, ok := rpn["total"].(float64); !ok {
		panic("total is not number")
	} else if data, ok := rpn["data"].([]interface{}); !ok {
		panic("data is not array")
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
		if float64(size) < total {
			fetchManyRecursive(endpoint, limiter, size, limit)
		}
	}
}

func fetchManyRecursive(endpoint string, limiter ratelimit.Limiter, offset int, limit int) {
	limiter.Take()
	if res, err := sendRequest(fmt.Sprint(endpoint, "limit=", limit, "&offset=", offset)); err != nil {
		panic(err)
	} else if rpn, ok := res.(map[string]interface{}); !ok {
		panic("Response is not object")
	} else if success, ok := rpn["success"].(bool); !ok || !success {
		panic("success is not true")
	} else if total, ok := rpn["total"].(float64); !ok {
		panic("total is not int")
	} else if data, ok := rpn["data"].([]interface{}); !ok {
		panic("data is not array")
	} else if size := len(data); size <= 0 {
		return
	} else {
		for i := 0; i < size; i++ {
			if out, err := json.Marshal(data[i]); err != nil {
				panic(err)
			} else {
				fmt.Print(",")
				fmt.Print(string(out))
			}
		}
		if float64(offset+size) < total {
			fetchManyRecursive(endpoint, limiter, offset+size, limit)
		}
	}
}
