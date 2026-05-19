package scanner

import (
	"errors"
	"fmt"
	"regexp"

	"gopkg.in/yaml.v3"

	"github.com/titlis/prbot/internal/model"
)

var (
	ErrInvalidAPIVersion = errors.New("invalid apiVersion")
	ErrInvalidKind       = errors.New("invalid kind")
	ErrMissingPaths      = errors.New("spec.gitops.paths must cover at least one environment")
	ErrUnsupportedLayout = errors.New("unsupported gitops layout")
)

const (
	expectedAPIVersion = "titlis.io/v1"
	expectedKind       = "ServiceDefinition"
)

func ParseServiceDefinition(input []byte) (model.ServiceDefinition, error) {
	var def model.ServiceDefinition
	if err := yaml.Unmarshal(input, &def); err != nil {
		return model.ServiceDefinition{}, fmt.Errorf("yaml: %w", err)
	}
	if def.APIVersion != expectedAPIVersion {
		return model.ServiceDefinition{}, ErrInvalidAPIVersion
	}
	if def.Kind != expectedKind {
		return model.ServiceDefinition{}, ErrInvalidKind
	}
	if def.Metadata.Name == "" {
		return model.ServiceDefinition{}, errors.New("metadata.name required")
	}
	if def.Spec.GitOps.Layout != "folder_per_env" {
		return model.ServiceDefinition{}, ErrUnsupportedLayout
	}
	if len(def.Spec.GitOps.Paths) == 0 {
		return model.ServiceDefinition{}, ErrMissingPaths
	}
	globalBranch := def.Spec.GitOps.BaseBranch
	if globalBranch == "" {
		globalBranch = "main"
	}
	def.Spec.GitOps.BaseBranch = globalBranch
	for envKey, spec := range def.Spec.GitOps.Paths {
		if envKey != string(model.EnvShortDev) && envKey != string(model.EnvShortHml) && envKey != string(model.EnvShortPrd) {
			return model.ServiceDefinition{}, fmt.Errorf("invalid env key in paths: %s", envKey)
		}
		if spec.Path == "" {
			return model.ServiceDefinition{}, fmt.Errorf("empty path for %s", envKey)
		}
		if spec.BaseBranch == "" {
			spec.BaseBranch = globalBranch
		}
		def.Spec.GitOps.Paths[envKey] = spec
	}
	if def.Metadata.WorkloadMatch.NamePattern != "" {
		if _, err := regexp.Compile(def.Metadata.WorkloadMatch.NamePattern); err != nil {
			return model.ServiceDefinition{}, fmt.Errorf("invalid name_pattern: %w", err)
		}
	}
	return def, nil
}

func MatchesWorkload(def model.ServiceDefinition, workloadName, namespace string) bool {
	if def.Metadata.Name == workloadName {
		return true
	}
	wm := def.Metadata.WorkloadMatch
	if len(wm.Namespaces) > 0 {
		found := false
		for _, n := range wm.Namespaces {
			if n == namespace {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	if wm.NamePattern == "" {
		return false
	}
	re, err := regexp.Compile(wm.NamePattern)
	if err != nil {
		return false
	}
	return re.MatchString(workloadName)
}
