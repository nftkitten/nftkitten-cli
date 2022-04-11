package magiceden

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"html/template"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
	"github.com/valyala/fasthttp"
	"go.uber.org/ratelimit"

	_ "github.com/lib/pq"
)

var Cmd = &cobra.Command{
	Use:     "magiceden",
	Example: "echo https://api-mainnet.magiceden.dev/v2/collections > LIMIT=500 ./nftkitten magiceden >.out.json; [ $? -eq 0 ] && mv .out.json out.json  || rm .out.json",
	Run: func(cmd *cobra.Command, args []string) {
		if rate := lookupEnvToI("RATE", 2); rate <= 0 {
			execute(2)
		} else {
			execute(rate)
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

func execute(rate int) {
	limiter := ratelimit.New(rate)
	sep := ""
	scanner := bufio.NewScanner(os.Stdin)

	for scanner.Scan() {
		if endpointText := strings.TrimSpace(os.ExpandEnv(scanner.Text())); endpointText != "" {
			if endpointTmpl, err := getTemplate(endpointText); err != nil {
				panic(err)
			} else {
				fetchMany(endpointTmpl, &sep, limiter)
			}
		}
	}

	if err := scanner.Err(); err != nil {
		panic(err)
	}

	log.Println("done")
}

func getTemplate(input string) (*template.Template, error) {
	if endpointText := strings.TrimSpace(os.ExpandEnv(input)); endpointText != "" {
		counters := map[string]int{}
		funcMap := template.FuncMap{
			"counter": func(key string, step int, start int) int {
				if val, ok := counters[key]; ok {
					counters[key] = val + step
				} else {
					counters[key] = start
				}
				return counters[key]
			},
			"noCounter": func(key string) bool {
				if _, ok := counters[key]; ok {
					return false
				} else {
					return true
				}
			},
		}
		if endpointTmpl, err := template.New("endpoint").Funcs(funcMap).Parse(endpointText); err != nil {
			return nil, err
		} else {
			return endpointTmpl, nil
		}
	}
	return nil, fmt.Errorf("invalid template")
}

func getRequest(tmpl *template.Template, data interface{}) (string, error) {
	var tpl bytes.Buffer
	if err := tmpl.Execute(&tpl, data); err != nil {
		return "", err
	} else {
		return tpl.String(), nil
	}
}

func printRow(row interface{}, sep *string) {
	s := *sep
	*sep = "\n"
	if out, err := json.Marshal(row); err != nil {
		panic(err)
	} else {
		os.Stdout.WriteString(s)
		os.Stdout.Write(out)
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

func fetchMany(endpointTmpl *template.Template, sep *string, limiter ratelimit.Limiter) {
	if endpoint, err := getRequest(endpointTmpl, map[string]interface{}{
		"lastEndpoint": "",
		"lastRecord":   nil,
	}); err != nil {
		panic(err)
	} else if endpoint != "" {
		limiter.Take()

		if res, err := sendRequest(endpoint); err != nil {
			panic(err)
		} else if res != nil {
			printRow(res, sep)
			fetchManyRecursive(endpointTmpl, endpoint, res, sep, limiter)
		}
	}
}

func fetchManyRecursive(endpointTmpl *template.Template, lastEndpoint string, lastRecord interface{}, sep *string, limiter ratelimit.Limiter) {
	if endpoint, err := getRequest(endpointTmpl, map[string]interface{}{
		"lastEndpoint": lastEndpoint,
		"lastRecord":   lastRecord,
	}); err != nil {
		panic(err)
	} else if endpoint != "" && endpoint != lastEndpoint {
		limiter.Take()

		if res, err := sendRequest(endpoint); err != nil {
			panic(err)
		} else if res != nil {
			printRow(res, sep)
			fetchManyRecursive(endpointTmpl, lastEndpoint, res, sep, limiter)
		}
	}
}
