module github.com/nftkitten/nftkitten

go 1.17

replace (
	github.com/nftkitten/nftkitten-cli/cmd/magiceden => ./cmd/magiceden
	github.com/nftkitten/nftkitten-cli/cmd/me => ./cmd/me
	github.com/nftkitten/nftkitten-cli/cmd/solscan => ./cmd/solscan
)

require (
	github.com/joho/godotenv v1.4.0
	github.com/nftkitten/nftkitten-cli/cmd/magiceden v0.0.0-00010101000000-000000000000
	github.com/nftkitten/nftkitten-cli/cmd/me v1.0.0
	github.com/nftkitten/nftkitten-cli/cmd/solscan v1.0.0
	github.com/spf13/cobra v1.4.0
)

require (
	github.com/andres-erbsen/clock v0.0.0-20160526145045-9e14626cd129 // indirect
	github.com/andybalholm/brotli v1.0.4 // indirect
	github.com/fatih/color v1.13.0 // indirect
	github.com/inconshreveable/mousetrap v1.0.0 // indirect
	github.com/klauspost/compress v1.15.1 // indirect
	github.com/lib/pq v1.10.4 // indirect
	github.com/mattn/go-colorable v0.1.12 // indirect
	github.com/mattn/go-isatty v0.0.14 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/stretchr/testify v1.7.1 // indirect
	github.com/tidwall/limiter v0.4.0 // indirect
	github.com/valyala/bytebufferpool v1.0.0 // indirect
	github.com/valyala/fasthttp v1.35.0 // indirect
	github.com/yalp/jsonpath v0.0.0-20180802001716-5cc68e5049a0 // indirect
	go.uber.org/ratelimit v0.2.0 // indirect
	golang.org/x/sys v0.0.0-20220317061510-51cd9980dadf // indirect
	gopkg.in/yaml.v3 v3.0.0-20210107192922-496545a6307b // indirect
)
