package common

import "encoding/json"

type Function struct {
	Kind     string `json:"kind"`
	Metadata FunctionMeta
	Spec     FunctionSpec
	Status   json.RawMessage `json:"status,omitempty"`
}

type FunctionMeta struct {
	Project     string `json:"project,omitempty"`
	Name        string `json:"name"`
	Tag         string `json:"tag,omitempty"`
	Namespace   string `json:"namespace,omitempty"`
	Labels      map[string]string
	Annotations map[string]string
}

type FunctionSpec struct {
	Command     string          `json:"command,omitempty"`
	Image       string          `json:"image,omitempty"`
	Mode        string          `json:"mode,omitempty"`
	Args        []string        `json:"args,omitempty"`
	Description string          `json:"description,omitempty"`
	Build       ImageBuilder    `json:"build,omitempty"`
	EnrtyPoints json.RawMessage `json:"entry_points,omitempty"`

	Env             json.RawMessage `json:"env,omitempty"`
	Volumes         json.RawMessage `json:"volumes,omitempty"`
	VolumeMounts    json.RawMessage `json:"volume_mounts,omitempty"`
	Resources       json.RawMessage `json:"resources,omitempty"`
	Replicas        int             `json:"replicas,omitempty"`
	ImagePullPolicy string          `json:"image_pull_policy,omitempty"`
	ServiceAccount  string          `json:"service_account,omitempty"`
}

type ImageBuilder struct {
	BaseImage          string   `json:"base_image,omitempty"`
	FunctionSourceCode []byte   `json:"functionSourceCode,omitempty"`
	Commands           []string `json:"commands,omitempty"`
	Registry           string   `json:"registry,omitempty"`
	Secret             string   `json:"secret,omitempty"`
	Source             string   `json:"source,omitempty"`
	Image              string   `json:"image,omitempty"`
}

func MergeMaps(one, two map[string]string) {
	for k, v := range two {
		one[k] = v
	}
}

func MergeStrings(one *string, two string) {
	if two != "" {
		*one = two
	}
}

func MergeRawJson(one *json.RawMessage, two json.RawMessage) {
	if len(two) > 0 {
		*one = two
	}
}

func MergeFunctions(one, two *Function) {
	MergeStrings(&one.Metadata.Project, two.Metadata.Project)
	MergeStrings(&one.Metadata.Name, two.Metadata.Name)
	MergeStrings(&one.Metadata.Tag, two.Metadata.Tag)
	MergeMaps(one.Metadata.Labels, two.Metadata.Labels)
	MergeMaps(one.Metadata.Annotations, two.Metadata.Annotations)

	build := one.Spec.Build
	if build.BaseImage == "" && len(build.Commands) == 0 && len(build.FunctionSourceCode) == 0 {
		one.Spec.Build = two.Spec.Build
	}
	MergeRawJson(&one.Spec.EnrtyPoints, two.Spec.EnrtyPoints)
	MergeStrings(&one.Spec.Command, two.Spec.Command)
	MergeStrings(&one.Spec.Image, two.Spec.Image)
	MergeStrings(&one.Spec.Mode, two.Spec.Mode)
	MergeStrings(&one.Spec.Description, two.Spec.Description)
	if len(two.Spec.Args) > 0 {
		one.Spec.Args = two.Spec.Args
	}
	MergeRawJson(&one.Spec.Env, two.Spec.Env)
	MergeRawJson(&one.Spec.Volumes, two.Spec.Volumes)
	MergeRawJson(&one.Spec.VolumeMounts, two.Spec.VolumeMounts)
	MergeRawJson(&one.Spec.Resources, two.Spec.Resources)
	if two.Spec.Replicas > 0 {
		one.Spec.Replicas = two.Spec.Replicas
	}
	MergeStrings(&one.Spec.ImagePullPolicy, two.Spec.ImagePullPolicy)
	MergeStrings(&one.Spec.ServiceAccount, two.Spec.ServiceAccount)
}
