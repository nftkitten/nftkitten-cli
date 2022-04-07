package main

import (
	"log"
	"os"

	"github.com/joho/godotenv"
	"github.com/nftkitten/nftkitten-cli/cmd/magiceden"
	"github.com/nftkitten/nftkitten-cli/cmd/me"
	"github.com/nftkitten/nftkitten-cli/cmd/solscan"
	"github.com/spf13/cobra"
)

func main() {
	if env := os.Getenv("NK_ENV"); env == "" {
		godotenv.Load()
	} else {
		log.Println("Loading .env." + env + ".local")
		godotenv.Load(".env." + env + ".local")
	}

	rootCmd := &cobra.Command{Use: "app"}
	rootCmd.AddCommand(me.Cmd)
	rootCmd.AddCommand(solscan.Cmd)
	rootCmd.AddCommand(magiceden.Cmd)
	rootCmd.Execute()
}
