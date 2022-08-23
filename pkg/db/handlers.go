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
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/ghodss/yaml"
	"github.com/tidwall/sjson"
	"github.com/v3io/v3io-go/pkg/dataplane"
	"github.com/v3io/v3io-go/pkg/errors"
	"github.com/valyala/fasthttp"
	"io"
	"math"
	"net/http"
	"os"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

var (
	invalidString = "?invalid"
	invalidFloat  = math.Inf(+1)
	invalidInt    = 0xBADACAFE

	container v3io.Container

	clog              = ConditionalPrinter{print: false, writer: os.Stderr}
	encodeRegex       = regexp.MustCompile(`[^a-zA-Z0-9_]`)
	labelParsingRegex = regexp.MustCompile(`(.+)(~=|!=|=)(.+)`)
)

type ConditionalPrinter struct {
	print  bool
	writer io.Writer
}

func (p ConditionalPrinter) printF(s string, params ...interface{}) {
	if p.print {
		if len(params) == 0 {
			fmt.Fprintf(p.writer, s)
		} else {
			fmt.Fprintf(p.writer, s, params...)
		}
	}
}

type dataDescriptor struct {
	path          string
	filterScalars []string
	filterLists   []string
}

const (
	dataAttributeName = "_data_"
)

type runMetadataEnvelope struct {
	Metadata struct {
		Name      string
		UID       string
		Iteration int
		Project   string
		Labels    map[string]string
	}
	Status struct {
		State     string
		LastTime  string `json:"last_update"`
		StartTime string `json:"start_time"`
	}
}

func (r *runMetadataEnvelope) makeInvalid() {
	r.Metadata.Name = invalidString
	r.Metadata.UID = invalidString
	r.Metadata.Project = invalidString
	r.Metadata.Labels = nil
	r.Metadata.Iteration = invalidInt
	r.Status.LastTime = invalidString
	r.Status.StartTime = invalidString
}

type artifactMetadataEnvelope struct {
	Name   string `json:"key"`
	Labels map[string]string
}

func (r *artifactMetadataEnvelope) makeInvalid() {
	r.Name = invalidString
	r.Labels = nil
}

func encodeAttributeName(name string) string {
	return encodeRegex.ReplaceAllString(name, "_")
}

func metadataToV3ioAttributes(md interface{}, attributePath string, result *map[string]interface{}) {
	st := reflect.TypeOf(md)
	sv := reflect.ValueOf(md)
	if st.Kind() == reflect.Ptr {
		st = reflect.TypeOf(md).Elem()
		sv = reflect.ValueOf(md).Elem()
	}
	if *result == nil {
		*result = make(map[string]interface{})
	}
	for i := 0; i < st.NumField(); i++ {
		field := st.Field(i)
		name := attributePath + strings.ToLower(field.Name)
		encodedName := encodeAttributeName(name)

		fieldValue := sv.Field(i)
		switch fieldValue.Kind() {
		case reflect.Struct:
			metadataToV3ioAttributes(fieldValue.Interface(), name+".", result)
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			if fieldValue.Int() != int64(invalidInt) {
				(*result)[encodedName] = fieldValue.Int()
			}
		case reflect.Bool:
			(*result)[encodedName] = fieldValue.Bool()
		case reflect.Float32, reflect.Float64:
			if fieldValue.Float() != invalidFloat {
				(*result)[encodedName] = fieldValue.Float()
			}
		case reflect.String:
			if fieldValue.String() != invalidString {
				// Replace time fields to Epoch integer representation
				t, err := time.Parse("2006-01-02 15:04:05.000000", fieldValue.String())
				if err != nil {
					(*result)[encodedName] = fieldValue.String()
				} else {
					encodedName := encodeAttributeName(name + "Epoch")
					(*result)[encodedName] = t.UnixNano()
				}
			}
		case reflect.Map:
			values := fieldValue.Interface().(map[string]string)
			for key, value := range values {
				encodedName := encodeAttributeName(name + "." + key)
				(*result)[encodedName] = value
			}
		default:
			clog.printF("metadataToV3ioAttributes : usupported type %v for attribute %s\n", fieldValue.Kind(), name)
		}
	}
}

func parseLabelToV3IOFilterSubexpression(labelPrefix string, text string) string {
	result := labelParsingRegex.FindStringSubmatch(text)
	// The attribute with no comparison means "exists"- text is the attribute name
	if labelPrefix != "" {
		labelPrefix = labelPrefix + "."
	}
	if len(result) != 4 {
		return "exists(" + encodeAttributeName(labelPrefix+text) + ")"
	}
	label := encodeAttributeName(labelPrefix + result[1])
	op := result[2]
	comp := result[3]
	switch op {
	case "~=":
		return "contains(" + label + ",'" + comp + "')"
	case "=":
		return label + "='" + comp + "'"
	case "!=":
		return label + "='" + comp + "'"
	}
	return "<unknown field>"
}

func buildRunFilterString(labels map[string]string, name string, state string, endPosixDate int64) string {
	result := ""
	if name != "" {
		if result != "" {
			result += " AND "
		}
		result += encodeAttributeName("metadata.name") + "== \"" + name + "\""
	}

	if state != "" {
		if result != "" {
			result += " AND "
		}
		result += encodeAttributeName("status.state") + "== \"" + state + "\""
	}

	for _, value := range labels {
		if result != "" {
			result += " AND "
		}
		result += value
	}
	if endPosixDate > 0 {
		if result != "" {
			result += " AND "
		}
		result += encodeAttributeName("status.lasttimeEpoch") + " > " + string(endPosixDate)
	}
	clog.printF("Filter string is %s\n", result)
	return result
}

func buildArtifactFilterString(labels map[string]string, name string, tag string) string {
	result := ""
	if name != "" {
		if result != "" {
			result += " AND "
		}
		result += encodeAttributeName("name") + "== \"" + name + "\""
	}

	if tag != "" {
		if result != "" {
			result += " AND "
		}
		result += "ends(__name,\"" + tag + "\")"
	}

	for _, value := range labels {
		if result != "" {
			result += " AND "
		}
		result += value
	}
	clog.printF("artifact Filter string is %s\n", result)
	return result
}
func isYAML(data []byte) bool {
	return bytes.HasPrefix(data, []byte("---"))
}

func storeLogHandler(ctx *fasthttp.RequestCtx) {
	project := ctx.UserValue("project")
	uid := ctx.UserValue("uid")
	clog.printF("storeLogHandler : Project %s uid %s\n", project, uid)
	logBody := ctx.Request.Body()

	putObjectInput := &v3io.PutObjectInput{}

	putObjectInput.Path = fmt.Sprintf("/log/%s-%s", project, uid)
	putObjectInput.Body = logBody

	err := container.PutObjectSync(putObjectInput)

	errWithStatusCode, _ := err.(v3ioerrors.ErrorWithStatusCode)
	ctx.Response.SetStatusCode(errWithStatusCode.StatusCode())
}

func getLogHandler(ctx *fasthttp.RequestCtx) {
	project := ctx.UserValue("project")
	uid := ctx.UserValue("uid")
	clog.printF("getLogHandler : Project %s uid %s\n", project, uid)

	getObjectInput := &v3io.GetObjectInput{}
	getObjectInput.Path = fmt.Sprintf("/log/%s-%s", project, uid)

	v3ioResponse, err := container.GetObjectSync(getObjectInput)

	errWithStatusCode, _ := err.(v3ioerrors.ErrorWithStatusCode)
	ctx.Response.SetStatusCode(errWithStatusCode.StatusCode())
	ctx.Response.SetBody(v3ioResponse.Body())
	v3ioResponse.Release()
}

func convertDataToJSON(data []byte) ([]byte, error) {
	if isYAML(data) {
		JSONData, err := yaml.YAMLToJSON(data)
		if err != nil {
			clog.printF("Failed to convert yaml to JSON :%s", err)
			return nil, err
		}
		return JSONData, nil
	}
	return data, nil
}

func storeMetadataObject(ctx *fasthttp.RequestCtx, path string, data []byte, attributesToAdd map[string]interface{}, descriptor interface{}) {
	JSONData, err := convertDataToJSON(data)
	if err != nil {
		clog.printF("storeRunHandler: Failed to convertDataToJSON: %s", err)
		ctx.Response.SetStatusCode(http.StatusBadRequest)
		return
	}
	err = json.Unmarshal(JSONData, descriptor)
	if err != nil {
		clog.printF("storeRunHandler: Failed to unmarshal run body: %s", err)
		ctx.Response.SetStatusCode(http.StatusBadRequest)
		return
	}

	updateItemInput := v3io.UpdateItemInput{}
	updateItemInput.Attributes = make(map[string]interface{})
	updateItemInput.Path = path
	for key, value := range attributesToAdd {
		updateItemInput.Attributes[key] = value
	}

	attributes := &updateItemInput.Attributes
	metadataToV3ioAttributes(descriptor, "", attributes)
	updateItemInput.Attributes[dataAttributeName] = data

	err = container.UpdateItemSync(&updateItemInput)
	if err != nil {
		clog.printF("storeRunHandler: Failed to call UpdateItemSync : %s", err)
	}
	errWithStatusCode, _ := err.(v3ioerrors.ErrorWithStatusCode)
	ctx.Response.SetStatusCode(errWithStatusCode.StatusCode())
}

func storeRunHandler(ctx *fasthttp.RequestCtx) {
	requestHandlerPrint(ctx)
	project := ctx.UserValue("project")
	uid := ctx.UserValue("uid")
	var updateMetadata = runMetadataEnvelope{}
	updateMetadata.makeInvalid()
	specialAttributes := map[string]interface{}{}
	storeMetadataObject(ctx, fmt.Sprintf("/run/%s/%s", project, uid), ctx.Request.Body(), specialAttributes, &updateMetadata)
}

func updateRunHandler(ctx *fasthttp.RequestCtx) {
	requestHandlerPrint(ctx)
	project := ctx.UserValue("project")
	uid := ctx.UserValue("uid")
	clog.printF("updateRunHandler : Project %s uid %s\n", project, uid)
	updateJSONBody, err := convertDataToJSON(ctx.Request.Body())
	if err != nil {
		clog.printF("updateRunHandler: Failed to convertDataToJSON: %s", err)
		ctx.Response.SetStatusCode(http.StatusBadRequest)
		return
	}

	updateJSONBodyUndecorated, err := dotSeparatedPathToJSON(updateJSONBody, []byte(""))
	if err != nil {
		clog.printF("updateRunHandler: Failed to call dotSeparatedPathToJSON : %s", err)
		ctx.Response.SetStatusCode(http.StatusBadRequest)
		return
	}

	var updateMetadata runMetadataEnvelope
	updateMetadata.makeInvalid()
	json.Unmarshal(updateJSONBodyUndecorated, &updateMetadata)

	getItemInput := &v3io.GetItemInput{
		Path:           fmt.Sprintf("/run/%s/%s", project, uid),
		AttributeNames: []string{dataAttributeName},
	}

	v3ioResponse, err := container.GetItemSync(getItemInput)
	if err != nil {
		clog.printF("updateRunHandler: Failed to read existing object: %s", err)
		errWithStatusCode, _ := err.(v3ioerrors.ErrorWithStatusCode)
		ctx.Response.SetStatusCode(errWithStatusCode.StatusCode())
		ctx.Response.SetBody(v3ioResponse.Body())
		v3ioResponse.Release()
		return
	}
	getItemOutput := v3ioResponse.Output.(*v3io.GetItemOutput)
	oldBody := getItemOutput.Item[dataAttributeName].([]byte)
	v3ioResponse.Release()
	oldJSONBody, err := convertDataToJSON(oldBody)
	if err != nil {
		clog.printF("updateRunHandler: Failed to convertDataToJSON: %s", err)
		ctx.Response.SetStatusCode(http.StatusBadRequest)
		return
	}

	newJSONBody, err := dotSeparatedPathToJSON(updateJSONBody, oldJSONBody)
	if err != nil {
		clog.printF("updateRunHandler: Failed to call dotSeparatedPathToJSON : %s", err)
		ctx.Response.SetStatusCode(http.StatusBadRequest)
		return
	}

	updateItemInput := v3io.UpdateItemInput{}
	updateItemInput.Path = getItemInput.Path
	metadataToV3ioAttributes(updateMetadata, "", &updateItemInput.Attributes)
	println(updateItemInput.Attributes)
	if isYAML(oldBody) {
		newYamlBody, err := yaml.JSONToYAML(newJSONBody)
		if err != nil {
			clog.printF("updateRunHandler: Failed to call JSONToYAML : %s", err)
			ctx.Response.SetStatusCode(http.StatusBadRequest)
			return
		}
		updateItemInput.Attributes[dataAttributeName] = newYamlBody
	} else {
		updateItemInput.Attributes[dataAttributeName] = newJSONBody
	}
	err = container.UpdateItemSync(&updateItemInput)
	if err != nil {
		clog.printF("updateRunHandler: Failed to call UpdateItemSync : %s", err)
	}
	errWithStatusCode, _ := err.(v3ioerrors.ErrorWithStatusCode)
	ctx.Response.SetStatusCode(errWithStatusCode.StatusCode())
}

func readMetadataObject(ctx *fasthttp.RequestCtx, path string) {
	getItemInput := &v3io.GetItemInput{
		Path:           path,
		AttributeNames: []string{dataAttributeName},
	}

	v3ioResponse, err := container.GetItemSync(getItemInput)
	if err != nil {
		errWithStatusCode, _ := err.(v3ioerrors.ErrorWithStatusCode)
		ctx.Response.SetStatusCode(errWithStatusCode.StatusCode())
		ctx.Response.SetBody(v3ioResponse.Body())
		v3ioResponse.Release()
		return
	}
	getItemOutput := v3ioResponse.Output.(*v3io.GetItemOutput)
	body := getItemOutput.Item[dataAttributeName].([]byte)
	v3ioResponse.Release()
	body = append([]byte("{\"data\":"), body...)
	body = append(body, "}"...)
	ctx.Response.SetBody(body)
}

func readRunHandler(ctx *fasthttp.RequestCtx) {
	requestHandlerPrint(ctx)
	project := ctx.UserValue("project")
	uid := ctx.UserValue("uid")
	clog.printF("readRunHandler : Project %s uid %s\n", project, uid)

	readMetadataObject(ctx, fmt.Sprintf("/run/%s/%s", project, uid))
}

func deleteRunHandler(ctx *fasthttp.RequestCtx) {
	requestHandlerPrint(ctx)
	project := ctx.UserValue("project")
	uid := ctx.UserValue("uid")
	clog.printF("deleteRunHandler : Project %s uid %s\n", project, uid)

	deleteItemInput := &v3io.DeleteObjectInput{
		Path: fmt.Sprintf("/run/%s/%s", project, uid),
	}
	err := container.DeleteObjectSync(deleteItemInput)
	errWithStatusCode, _ := err.(v3ioerrors.ErrorWithStatusCode)
	ctx.Response.SetStatusCode(errWithStatusCode.StatusCode())
}

func listRunsHandler(ctx *fasthttp.RequestCtx) {
	requestHandlerPrint(ctx)
	doSort := string(ctx.QueryArgs().Peek("sort"))
	last, err := strconv.Atoi(string(ctx.QueryArgs().Peek("last")))
	if err == nil {
		last = 30 // Same as in python code
	}

	project := string(ctx.QueryArgs().Peek("project"))
	if project == "" {
		clog.printF("listRunsHandler : Expecting 'project' parameter")
		ctx.Response.SetStatusCode(http.StatusBadRequest)
		return
	}

	labelsParams := ctx.QueryArgs().PeekMulti("label")
	var labels map[string]string
	for key, value := range labelsParams {
		labels[string(key)] = parseLabelToV3IOFilterSubexpression("metadata", string(value))
	}

	filterStr := buildRunFilterString(labels,
		string(ctx.QueryArgs().Peek("name")),
		string(ctx.QueryArgs().Peek("state")),
		-1)

	getItemsInput := v3io.GetItemsInput{
		Path:           fmt.Sprintf("/run/%s/", project),
		AttributeNames: []string{"__name", dataAttributeName, encodeAttributeName("status.starttimeEpoch")},
		Filter:         filterStr,
	}

	cursor, err := v3io.NewItemsCursor(container, &getItemsInput)
	if err != nil {
		if err.(v3ioerrors.ErrorWithStatusCode).StatusCode() == http.StatusNotFound {
			//Directory not found! Return an empty list
			result := []byte("{\"runs\": []}")
			println(string(result))
			ctx.Response.SetBody([]byte(result))
			return
		}
		clog.printF("listRunHandler: Failed to call NewItemsCursor : %s", err)
		errWithStatusCode, _ := err.(v3ioerrors.ErrorWithStatusCode)
		ctx.Response.SetStatusCode(errWithStatusCode.StatusCode())
		return
	}

	result := []byte("{\"runs\": [")
	cursorItems, err := cursor.AllSync()
	resultMapByTime := make(map[int][]byte)
	dummyTimestampEpoch := 0
	var keys []int
	for _, cursorItem := range cursorItems {
		key, err := cursorItem.GetFieldInt(encodeAttributeName("status.lasttimeEpoch"))
		if err != nil {
			key = dummyTimestampEpoch
			dummyTimestampEpoch++
		}
		keys = append(keys, key)

		md := cursorItem.GetField(dataAttributeName).([]byte)
		resultMapByTime[key] = md
	}
	if doSort == "true" || last != 0 {
		sort.Slice(keys, func(i, j int) bool { return keys[i] > keys[j] })
	}
	numOfKeysLeftToAdd := len(keys)
	if last != 0 && numOfKeysLeftToAdd > last {
		numOfKeysLeftToAdd = last
	}
	keysToAdd := keys[:numOfKeysLeftToAdd]
	for _, key := range keysToAdd {
		numOfKeysLeftToAdd--
		result = append(result, resultMapByTime[key]...)
		if numOfKeysLeftToAdd > 0 {
			result = append(result, ","...)
		}
	}
	result = append(result, "]}"...)
	println(string(result))
	ctx.Response.SetBody([]byte(result))
}

func deleteRunsHandler(ctx *fasthttp.RequestCtx) {
	requestHandlerPrint(ctx)

	labelsParams := ctx.QueryArgs().PeekMulti("label")
	var labels map[string]string
	for key, value := range labelsParams {
		labels[string(key)] = parseLabelToV3IOFilterSubexpression("", string(value))
	}
	project := string(ctx.QueryArgs().Peek("project"))
	if project == "" {
		clog.printF("deleteRunsHandler : Expecting 'project' parameter")
		ctx.Response.SetStatusCode(http.StatusBadRequest)
		return
	}

	filterStr := buildRunFilterString(labels,
		string(ctx.QueryArgs().Peek("name")),
		string(ctx.QueryArgs().Peek("state")),
		-1)

	getItemsInput := v3io.GetItemsInput{
		Path:           fmt.Sprintf("/run/%s/", project),
		AttributeNames: []string{"__name"},
		Filter:         filterStr,
	}

	cursor, err := v3io.NewItemsCursor(container, &getItemsInput)
	if err != nil {
		clog.printF("deleteRunsHandler: Failed to call NewItemsCursor : %s", err)
		errWithStatusCode, _ := err.(v3ioerrors.ErrorWithStatusCode)
		ctx.Response.SetStatusCode(errWithStatusCode.StatusCode())
		return
	}
	var allErrors error
	allErrors = nil
	cursorItems, err := cursor.AllSync()
	for _, cursorItem := range cursorItems {
		name, _ := cursorItem.GetFieldString("__name")
		deleteItemInput := &v3io.DeleteObjectInput{
			Path: fmt.Sprintf("/run/%s/%s", project, name),
		}
		clog.printF("Deleting %s\n", name)
		err := container.DeleteObjectSync(deleteItemInput)
		if err != nil {
			allErrors = err
		}
	}
	errWithStatusCode, _ := allErrors.(v3ioerrors.ErrorWithStatusCode)
	ctx.Response.SetStatusCode(errWithStatusCode.StatusCode())
}

func storeArtifactHandler(ctx *fasthttp.RequestCtx) {
	requestHandlerPrint(ctx)
	project := ctx.UserValue("project")
	uid := ctx.UserValue("uid")
	key := string(ctx.QueryArgs().Peek("key"))
	if key == "" {
		clog.printF("storeArtifactHandler : Expecting 'key' parameter")
		ctx.Response.SetStatusCode(http.StatusBadRequest)
		return
	}
	tag := string(ctx.QueryArgs().Peek("tag"))

	if tag == "" {
		tag = "latest"
	}
	var updateMetadata = artifactMetadataEnvelope{}
	updateMetadata.makeInvalid()
	specialAttributes := map[string]interface{}{"name": key}
	storeMetadataObject(ctx, fmt.Sprintf("/artifact/%s/%s.%s", project, key, uid), ctx.Request.Body(), specialAttributes, &updateMetadata)
	updateMetadata.makeInvalid()
	storeMetadataObject(ctx, fmt.Sprintf("/artifact/%s/%s.%s", project, key, tag), ctx.Request.Body(), specialAttributes, &updateMetadata)
}

func getArtifactHandler(ctx *fasthttp.RequestCtx) {
	requestHandlerPrint(ctx)
	project := ctx.UserValue("project")
	key := string(ctx.QueryArgs().Peek("key"))
	if key == "" {
		clog.printF("getArtifactHandler : Expecting 'key' parameter")
		ctx.Response.SetStatusCode(http.StatusBadRequest)
		return
	}
	tag := string(ctx.QueryArgs().Peek("tag"))
	if tag == "" {
		tag = "latest"
	}
	readMetadataObject(ctx, fmt.Sprintf("/artifact/%s/%s.%s", project, key, tag))
}

func deleteArtifactHandler(ctx *fasthttp.RequestCtx) {
	requestHandlerPrint(ctx)
	project := ctx.UserValue("project")
	key := string(ctx.QueryArgs().Peek("key"))
	if key == "" {
		clog.printF("getArtifactHandler : Expecting 'key' parameter")
		ctx.Response.SetStatusCode(http.StatusBadRequest)
		return
	}
	tag := string(ctx.QueryArgs().Peek("tag"))
	if tag == "" {
		tag = "latest"
	}
	deleteItemInput := &v3io.DeleteObjectInput{
		Path: fmt.Sprintf("/artifact/%s/%s.%s", project, key, tag),
	}
	err := container.DeleteObjectSync(deleteItemInput)
	errWithStatusCode, _ := err.(v3ioerrors.ErrorWithStatusCode)
	ctx.Response.SetStatusCode(errWithStatusCode.StatusCode())
}

func listArtifactsHandler(ctx *fasthttp.RequestCtx) {
	requestHandlerPrint(ctx)
	project := string(ctx.QueryArgs().Peek("project"))
	if project == "" {
		clog.printF("listArtifactsHandler : Expecting 'project' parameter")
		ctx.Response.SetStatusCode(http.StatusBadRequest)
		return
	}

	tag := string(ctx.QueryArgs().Peek("tag"))
	if tag == "" {
		tag = "latest"
	}
	if tag == "*" {
		tag = ""
	}

	labelsParams := ctx.QueryArgs().PeekMulti("label")

	var labels map[string]string
	for key, value := range labelsParams {
		labels[string(key)] = parseLabelToV3IOFilterSubexpression("metadata", string(value))
	}

	filterStr := buildArtifactFilterString(labels,
		string(ctx.QueryArgs().Peek("name")),
		tag)

	getItemsInput := v3io.GetItemsInput{
		Path:           fmt.Sprintf("/artifact/%s/", project),
		AttributeNames: []string{dataAttributeName},
		Filter:         filterStr,
	}

	cursor, err := v3io.NewItemsCursor(container, &getItemsInput)
	if err != nil {
		if err.(v3ioerrors.ErrorWithStatusCode).StatusCode() == http.StatusNotFound {
			//Directory not found! Return an empty list
			result := []byte("{\"artifacts\": []}")
			println(string(result))
			ctx.Response.SetBody([]byte(result))
			return
		}
		clog.printF("listArtifactsHandler: Failed to call NewItemsCursor : %s", err)
		errWithStatusCode, _ := err.(v3ioerrors.ErrorWithStatusCode)
		ctx.Response.SetStatusCode(errWithStatusCode.StatusCode())
		return
	}
	result := []byte("{\"artifacts\": [")
	cursorItems, err := cursor.AllSync()
	if err != nil {
		clog.printF("listArtifactsHandler: Failed to call cursor.AllSync : %s", err)
		errWithStatusCode, _ := err.(v3ioerrors.ErrorWithStatusCode)
		ctx.Response.SetStatusCode(errWithStatusCode.StatusCode())
		return
	}
	first := true
	for _, cursorItem := range cursorItems {
		if !first {
			result = append(result, ","...)
		}
		first = false
		md := cursorItem.GetField(dataAttributeName).([]byte)
		result = append(result, md...)
	}
	result = append(result, "]}"...)
	println(string(result))
	ctx.Response.SetBody([]byte(result))
}

func deleteArtifactsHandler(ctx *fasthttp.RequestCtx) {
	requestHandlerPrint(ctx)
	project := string(ctx.QueryArgs().Peek("project"))
	if project == "" {
		clog.printF("deleteArtifactsHandler : Expecting 'project' parameter")
		ctx.Response.SetStatusCode(http.StatusBadRequest)
		return
	}
	tag := string(ctx.QueryArgs().Peek("tag"))
	if tag == "" {
		tag = "latest"
	}
	if tag == "*" {
		tag = ""
	}

	labelsParams := ctx.QueryArgs().PeekMulti("label")

	var labels map[string]string
	for key, value := range labelsParams {
		labels[string(key)] = parseLabelToV3IOFilterSubexpression("metadata", string(value))
	}

	filterStr := buildArtifactFilterString(labels,
		string(ctx.QueryArgs().Peek("name")),
		tag)

	getItemsInput := v3io.GetItemsInput{
		Path:           fmt.Sprintf("/artifact/%s/", project),
		AttributeNames: []string{"__name"},
		Filter:         filterStr,
	}

	cursor, err := v3io.NewItemsCursor(container, &getItemsInput)
	if err != nil {
		if err.(v3ioerrors.ErrorWithStatusCode).StatusCode() == http.StatusNotFound {
			return
		}
		clog.printF("deleteArtifactsHandler: Failed to call NewItemsCursor : %s", err)
		errWithStatusCode, _ := err.(v3ioerrors.ErrorWithStatusCode)
		ctx.Response.SetStatusCode(errWithStatusCode.StatusCode())
		return
	}
	cursorItems, err := cursor.AllSync()
	if err != nil {
		clog.printF("deleteArtifactsHandler: Failed to call cursor.AllSync : %s", err)
		errWithStatusCode, _ := err.(v3ioerrors.ErrorWithStatusCode)
		ctx.Response.SetStatusCode(errWithStatusCode.StatusCode())
		return
	}
	var allErrors error
	allErrors = nil
	for _, cursorItem := range cursorItems {
		name, _ := cursorItem.GetFieldString("__name")
		deleteItemInput := &v3io.DeleteObjectInput{
			Path: fmt.Sprintf("/artifact/%s/%s", project, name),
		}
		clog.printF("Deleteing %s\n", name)
		err := container.DeleteObjectSync(deleteItemInput)
		if err != nil {
			allErrors = err
		}
	}
	errWithStatusCode, _ := allErrors.(v3ioerrors.ErrorWithStatusCode)
	ctx.Response.SetStatusCode(errWithStatusCode.StatusCode())
}

func requestHandlerPrint(ctx *fasthttp.RequestCtx) {
	queryArgsMap := make(map[string]string)
	ctx.Request.Header.VisitAll(func(key, value []byte) {
		queryArgsMap[string(key)] = string(value)
	})

	clog.printF("Request method is %q\n", ctx.Method())
	clog.printF("RequestURI is %q\n", ctx.RequestURI())
	clog.printF("Requested path is %q\n", ctx.Path())
	clog.printF("Host is %q\n", ctx.Host())
	clog.printF("Query string is %q\n", ctx.QueryArgs())

	clog.printF("User-Agent is %q\n", ctx.UserAgent())
	clog.printF("Connection has been established at %s\n", ctx.ConnTime())
	clog.printF("Request has been started at %s\n", ctx.Time())
	clog.printF("Serial request number for the current connection is %d\n", ctx.ConnRequestNum())
	clog.printF("Your ip is %q\n\n", ctx.RemoteIP())

	clog.printF("Raw request is:\n---CUT---\n%s\n---CUT---", &ctx.Request)
}

func requestHandler(ctx *fasthttp.RequestCtx) {
	requestHandlerPrint(ctx)
	ctx.SetContentType("text/plain; charset=utf8")

	// Set arbitrary headers
	ctx.Response.Header.Set("X-My-Header", "my-header-value")

	// Set cookies
	var c fasthttp.Cookie
	c.SetKey("cookie-name")
	c.SetValue("cookie-value")
	ctx.Response.Header.SetCookie(&c)
}

func dotSeparatedPathToJSON(dotSeparatedPatch []byte, jsonBody []byte) ([]byte, error) {
	var descriptor = make(map[string]interface{})
	err := json.Unmarshal([]byte(dotSeparatedPatch), &descriptor)
	if err != nil {
		return nil, err
	}
	for key, value := range descriptor {
		jsonBody, err = sjson.SetBytes(jsonBody, key, value)
		if err != nil {
			return nil, err
		}
	}
	return jsonBody, nil
}
