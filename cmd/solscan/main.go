package solscan

import (
	"log"

	"github.com/spf13/cobra"
)

var Cmd = &cobra.Command{
	Use:   "help",
	Short: "crawl Solscan API",
	Run: func(cmd *cobra.Command, args []string) {
		log.Println("to be implment.")
	},
}
