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
package builder

import (
	"encoding/base64"
	"fmt"
	"github.com/ghodss/yaml"
	"github.com/mlrun/controller/pkg/common"
	"io/ioutil"
	"os"
	"path/filepath"
)

const (
	functionEnvVar   = "MLRUN_FUNCTION_SPEC"
	defaultBaseImage = "python:3.6"
	mlrunPackage     = "mlrun"
)

type Opts struct {
	Verbose   []bool `short:"v" long:"verbose" description:"Show verbose debug information"`
	Source    string `short:"s" long:"source" description:"Source repo/path"`
	LocalPath string `short:"l" long:"local" description:"Local target path" required:"true"`
}

func InitBuildCtx(opts Opts) error {
	cfg := SourceConfig{Source: opts.Source, LocalPath: opts.LocalPath}
	codePath := opts.LocalPath
	if opts.Source != "" {
		repo, err := GetSourceRepo(&cfg)
		if err != nil {
			return err
		}
		err = repo.Download()
		if err != nil {
			return err
		}
		codePath = repo.CodePath()
	}

	function, err := getFunction(codePath)
	if err != nil {
		return err
	}
	fmt.Printf("F: %+v\n", function)
	code := function.Spec.Build.FunctionSourceCode
	if len(code) > 0 {
		funcFilePath := filepath.Join(codePath, "main.py")
		err = ioutil.WriteFile(funcFilePath, code, 0644)
		if err != nil {
			fmt.Printf("failed to write code: %+v\n", err)
		}
	}

	err = writeDockerfile(codePath, function)
	return err
}

func writeDockerfile(codePath string, function *common.Function) error {
	dockerfilePath := filepath.Join(codePath, "Dockerfile")
	if common.FileExists(dockerfilePath) {
		fmt.Println("Found Dockerfile")
		return nil
	}

	build := function.Spec.Build
	image := defaultBaseImage
	if build.BaseImage != "" {
		image = build.BaseImage
	}
	cmds := build.Commands
	pkgPath, valid := os.LookupEnv("MLRUN_PACKAGE_PATH")
	if !valid {
		pkgPath = mlrunPackage
	}
	cmds = append(cmds, "pip install "+pkgPath)
	dock := fmt.Sprintf("FROM %s\nWORKDIR /run\n", image)
	dock += fmt.Sprintf("ADD %s /run\n", codePath)
	for _, cmd := range cmds {
		dock += fmt.Sprintf("RUN %s\n", cmd)
	}
	dock += "ENV PYTHONPATH /run\n"
	fmt.Println(dock)
	err := ioutil.WriteFile(dockerfilePath, []byte(dock), 0644)
	return err
}

func getFunction(codePath string) (*common.Function, error) {

	var envFunc, repoFunc common.Function

	envFunc = common.Function{}
	func_str, gotFunction := os.LookupEnv(functionEnvVar)
	if gotFunction {
		data, err := base64.StdEncoding.DecodeString(func_str)
		if err != nil {
			return nil, err
		}
		fmt.Println(data)
		err = yaml.Unmarshal(data, &envFunc)
		if err != nil {
			return nil, err
		}
	}

	yamlPath := filepath.Join(codePath, "function.yaml")
	if common.FileExists(yamlPath) {
		data, err := ioutil.ReadFile(yamlPath)
		if err != nil {
			return nil, err
		}
		err = yaml.Unmarshal(data, &repoFunc)
		if err != nil {
			return nil, err
		}

		if gotFunction {
			common.MergeFunctions(&repoFunc, &envFunc)
		}

	} else {
		repoFunc = envFunc
	}

	return &repoFunc, nil
}
