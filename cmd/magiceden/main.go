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

	"github.com/yalp/jsonpath"
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
	scanner := bufio.NewScanner(os.Stdin)
	outputs := make(map[string]*os.File)

	for scanner.Scan() {
		if endpointTmpl, err := getTemplate(scanner.Text()); err != nil {
			panic(err)
		} else {
			fetchMany(endpointTmpl, limiter, outputs)
		}
	}

	if err := scanner.Err(); err != nil {
		panic(err)
	}

	closeOutput(outputs)
	log.Println("done")
}

func writeOutput(outName string, outputs map[string]*os.File, row interface{}) {
	outFile, ok := outputs[outName]

	if !ok {
		if outName != "" {
			var err error
			if outFile, err = os.OpenFile(outName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755); err != nil {
				panic(err)
			}
		} else {
			outFile = os.Stdout
		}

		if separator, ok := os.LookupEnv("START"); ok {
			outFile.WriteString(separator)
		}
	} else {
		if separator, ok := os.LookupEnv("SEPARATOR"); ok {
			outFile.WriteString(separator)
		} else {
			outFile.WriteString("\n")
		}
	}

	if out, err := json.Marshal(row); err != nil {
		panic(err)
	} else {
		outFile.Write(out)
	}
}

func closeOutput(outputs map[string]*os.File) {
	separator, _ := os.LookupEnv("END")

	for outName, val := range outputs {
		if separator != "" {
			val.WriteString(separator)
		}
		val.WriteString(separator)
		if outName != "" {
			val.Close()
		}
	}
}

func getTemplate(input string) (*template.Template, error) {
	if text := strings.TrimSpace(os.ExpandEnv(input)); text != "" {
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
			"last": func(records interface{}) interface{} {
				if array, ok := records.([]interface{}); ok && len(array) > 0 {
					return array[len(array)-1]
				} else {
					return nil
				}
			},
		}
		if tmpl, err := template.New("endpoint").Funcs(funcMap).Parse(text); err != nil {
			return nil, err
		} else {
			return tmpl, nil
		}
	}
	return nil, fmt.Errorf("invalid template")
}

func executeTmpl(tmpl *template.Template, data interface{}) string {
	if tmpl == nil {
		return ""
	} else {
		var tpl bytes.Buffer
		if err := tmpl.Execute(&tpl, data); err != nil {
			panic(err)
		} else {
			return tpl.String()
		}
	}
}

func processRow(outName string, row interface{}, outputs map[string]*os.File) {
	if path, ok := os.LookupEnv("JSONPATH"); ok && path != "" {
		if actual, err := jsonpath.Read(row, path); err != nil {
			panic(err)
		} else {
			row = actual
		}
	}

	if splitted, ok := row.([]interface{}); ok {
		for _, splittedRow := range splitted {
			writeOutput(outName, outputs, splittedRow)
		}
	} else {
		writeOutput(outName, outputs, row)
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

func fetchMany(endpointTmpl *template.Template, limiter ratelimit.Limiter, outputs map[string]*os.File) {
	data := map[string]interface{}{
		"lastEndpoint": "",
		"lastRecord":   nil,
	}
	if endpoint := executeTmpl(endpointTmpl, data); endpoint != "" {
		var err error
		var outTmpl *template.Template = nil

		if outText, ok := os.LookupEnv("OUT"); ok {
			if outTmpl, err = getTemplate(outText); err != nil {
				panic(err)
			}
		}

		limiter.Take()

		if res, err := sendRequest(endpoint); err != nil {
			panic(err)
		} else if res != nil {
			data = map[string]interface{}{
				"lastEndpoint": endpoint,
				"lastRecord":   res,
			}
			processRow(executeTmpl(outTmpl, data), res, outputs)
			fetchManyRecursive(endpointTmpl, outTmpl, data, endpoint, res, limiter, outputs)
		}
	}
}

func fetchManyRecursive(endpointTmpl *template.Template, outTmpl *template.Template, data interface{}, lastEndpoint string, lastRecord interface{}, limiter ratelimit.Limiter, outputs map[string]*os.File) {
	if endpoint := executeTmpl(endpointTmpl, data); endpoint != "" && endpoint != lastEndpoint {
		limiter.Take()

		if res, err := sendRequest(endpoint); err != nil {
			panic(err)
		} else if res != nil {
			data = map[string]interface{}{
				"lastEndpoint": endpoint,
				"lastRecord":   res,
			}
			processRow(executeTmpl(outTmpl, data), res, outputs)
			fetchManyRecursive(endpointTmpl, outTmpl, data, lastEndpoint, res, limiter, outputs)
		}
	}
}
