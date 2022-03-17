package me

import (
	"os"

	"github.com/fatih/color"
)

func logError(err error) {
	color.New(color.FgHiMagenta).Fprintln(os.Stderr, err.Error())
}

func doNothingOnNext(_ interface{}) {
	// nothing
}

func doNothing() {
	// nothing
}
