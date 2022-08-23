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
package db

import (
	"github.com/buaazp/fasthttprouter"
	"github.com/nuclio/zap"
	"github.com/v3io/v3io-go/pkg/dataplane"
	"github.com/v3io/v3io-go/pkg/dataplane/http"
)

type DBConfig struct {
	Endpoint  string
	Container string
	AccessKey string
}

func InitDB(config *DBConfig) (*MLRunDB, error) {
	newContainer, err := createContainer(config)
	if err != nil {
		return &MLRunDB{}, nil
	}
	container = newContainer // TODO: should use class and container as part of it
	return &MLRunDB{cfg: config, container: newContainer}, nil
}

type MLRunDB struct {
	cfg       *DBConfig
	container v3io.Container
}

func (db *MLRunDB) RegisterHandlers(router *fasthttprouter.Router) {
	router.POST("/log/:project/:uid", storeLogHandler)
	router.GET("/log/:project/:uid", getLogHandler)
	router.POST("/run/:project/:uid", storeRunHandler)
	router.PATCH("/run/:project/:uid", updateRunHandler)
	router.GET("/run/:project/:uid", readRunHandler)
	router.DELETE("/run/:project/:uid", deleteRunHandler)
	router.GET("/runs", listRunsHandler)
	router.DELETE("/runs", deleteRunsHandler)
	router.POST("/artifact/:project/:uid", storeArtifactHandler)
	router.GET("/artifact/:project", getArtifactHandler)
	router.DELETE("/artifact/:project", deleteArtifactHandler)
	router.GET("/artifacts", listArtifactsHandler)
	router.DELETE("/artifacts", deleteArtifactsHandler)
}

func createContainer(config *DBConfig) (v3io.Container, error) {
	var logger *nucliozap.NuclioZap
	var context v3io.Context
	var session v3io.Session
	var err error
	if logger, err = nucliozap.NewNuclioZapCmd("mlrunhttp", nucliozap.DebugLevel); err != nil {
		return nil, err
	}

	if context, err = v3iohttp.NewContext(logger, &v3io.NewContextInput{ClusterEndpoints: []string{config.Endpoint}}); err != nil {
		return nil, err
	}

	if session, err = context.NewSession(&v3io.NewSessionInput{AccessKey: config.AccessKey}); err != nil {
		return nil, err
	}

	return session.NewContainer(&v3io.NewContainerInput{ContainerName: config.Container})
}
