package main

import (
	"log"

	"github.com/joho/godotenv"
	"github.com/nftkitten/nftkitten-cli/cmd/me"
	"github.com/nftkitten/nftkitten-cli/cmd/solscan"
	"github.com/spf13/cobra"
)

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatalln("Error loading .env file")
	}

	var rootCmd = &cobra.Command{Use: "app"}
	rootCmd.AddCommand(me.Cmd)
	rootCmd.AddCommand(solscan.Cmd)
	rootCmd.Execute()
}
