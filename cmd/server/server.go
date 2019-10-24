package main

import (
	"github.com/jessevdk/go-flags"
	"github.com/mlrun/controller/pkg/server"
)

func main() {
	var opts server.ServerOpts
	_, err := flags.Parse(&opts)

	if err != nil {
		panic(err)
	}

	err = server.StartServer(&opts)
	if err != nil {
		panic(err)
	}
}
