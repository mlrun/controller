package main

import (
	"github.com/jessevdk/go-flags"
	"github.com/mlrun/controller/pkg/builder"
	"os"
)

func main() {
	var opts builder.Opts
	_, err := flags.ParseArgs(&opts, os.Args)

	if err != nil {
		panic(err)
	}

	err = builder.InitBuildCtx(opts)
	if err != nil {
		panic(err)
	}
}
