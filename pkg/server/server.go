/*
Copyright 2019 Iguazio Systems Ltd.

Licensed under the Apache License, Version 2.0 (the "License") with
an addition restriction as set forth herein. You may not use this
file except in compliance with the License. You may obtain a copy of
the License at http://www.apache.org/licenses/LICENSE-2.0.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License.

In addition, you may not use the software for any purposes that are
illegal under applicable law, and the grant of the foregoing license
under the Apache 2.0 license is conditioned upon your compliance with
such restriction.
*/
package server

import (
	"fmt"
	"github.com/buaazp/fasthttprouter"
	"github.com/mlrun/controller/pkg/db"
	"github.com/valyala/fasthttp"
	"log"
	"os"
)

// TODO: specify port vs server addr:port
type ServerOpts struct {
	Addr          string
	V3ioEndpoint  string
	ContainerName string
	AccessKey     string
}

func getEnvironmentVariables(cfg *ServerOpts) {
	if val, ok := os.LookupEnv("MLRUN_V3IO_DB_URL"); ok {
		cfg.Addr = val
	}
	if val, ok := os.LookupEnv("MLRUN_V3IO_DB_CONTAINER"); ok {
		cfg.ContainerName = val
	}
	if val, ok := os.LookupEnv("V3IO_ACCESS_KEY"); ok {
		cfg.AccessKey = val
	}
	if val, ok := os.LookupEnv("V3IO_API"); ok {
		cfg.V3ioEndpoint = fmt.Sprintf("http://%s", val)
	}
}

func StartServer(cfg *ServerOpts) error {

	getEnvironmentVariables(cfg)
	fmt.Printf("Address of the mlrun HTTP server : https://%s\n", cfg.Addr)
	fmt.Printf("Location of the v3io WebAPI: %s/%s\n", cfg.V3ioEndpoint, cfg.ContainerName)
	fmt.Printf("v3io WebAPI access key: %s\n", cfg.AccessKey)
	mldb, err := db.InitDB(&db.DBConfig{Endpoint: cfg.V3ioEndpoint, Container: cfg.ContainerName, AccessKey: cfg.AccessKey})

	router := fasthttprouter.New()
	router.GET("/healthz", healthHandler)

	mldb.RegisterHandlers(router)

	err = fasthttp.ListenAndServe(cfg.Addr, router.Handler)

	if err != nil {
		log.Fatalf("Error in ListenAndServe: %s", err)
	}
	return err
}

func healthHandler(ctx *fasthttp.RequestCtx) {
	fmt.Println("healthHandler\n")
}
