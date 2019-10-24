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
