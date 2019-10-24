package main

import (
	"github.com/jessevdk/go-flags"
	"github.com/mlrun/controller/pkg/builder"
)

func main() {
	var opts builder.Opts
	_, err := flags.Parse(&opts)

	if err != nil {
		panic(err)
	}

	err = builder.InitBuildCtx(opts)
	if err != nil {
		panic(err)
	}
}
