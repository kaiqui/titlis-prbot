package patch

import (
	"bytes"
	"fmt"

	"gopkg.in/yaml.v3"

	"github.com/titlis/prbot/internal/model"
)

// ApplyHpaRecommendation walks the YAML document and updates HPA-related fields.
// It expects either:
//   (a) a HorizontalPodAutoscaler manifest (kind == HorizontalPodAutoscaler), or
//   (b) a multi-doc manifest where one document is a HPA referring to the workload.
// Returns the patched YAML as bytes (formatted by yaml.v3).
func ApplyHpaRecommendation(input []byte, reco model.HpaRecommendation) ([]byte, error) {
	if reco.Source == string(model.PolicyDisabled) || reco.MinReplicas == 0 && reco.MaxReplicas == 0 && reco.TargetCPUPct == 0 {
		return nil, fmt.Errorf("recommendation is empty")
	}
	dec := yaml.NewDecoder(bytes.NewReader(input))
	var docs []*yaml.Node
	for {
		var n yaml.Node
		if err := dec.Decode(&n); err != nil {
			if err.Error() == "EOF" {
				break
			}
			return nil, fmt.Errorf("yaml decode: %w", err)
		}
		docs = append(docs, &n)
	}
	if len(docs) == 0 {
		return nil, fmt.Errorf("empty input")
	}

	mutated := false
	for _, d := range docs {
		if patchHPADocument(d, reco) {
			mutated = true
		}
	}
	if !mutated {
		return nil, fmt.Errorf("no HorizontalPodAutoscaler document found")
	}

	var buf bytes.Buffer
	enc := yaml.NewEncoder(&buf)
	enc.SetIndent(2)
	for _, d := range docs {
		if err := enc.Encode(d); err != nil {
			return nil, fmt.Errorf("yaml encode: %w", err)
		}
	}
	if err := enc.Close(); err != nil {
		return nil, fmt.Errorf("yaml close: %w", err)
	}
	return buf.Bytes(), nil
}

func patchHPADocument(doc *yaml.Node, reco model.HpaRecommendation) bool {
	root := documentRoot(doc)
	if root == nil || root.Kind != yaml.MappingNode {
		return false
	}
	kind := mapGet(root, "kind")
	if kind == nil || kind.Value != "HorizontalPodAutoscaler" {
		return false
	}
	spec := mapGet(root, "spec")
	if spec == nil || spec.Kind != yaml.MappingNode {
		return false
	}
	setScalar(spec, "minReplicas", fmt.Sprintf("%d", reco.MinReplicas))
	setScalar(spec, "maxReplicas", fmt.Sprintf("%d", reco.MaxReplicas))
	patchMetrics(spec, reco)
	return true
}

func patchMetrics(spec *yaml.Node, reco model.HpaRecommendation) {
	metrics := mapGet(spec, "metrics")
	if metrics == nil || metrics.Kind != yaml.SequenceNode {
		return
	}
	for _, m := range metrics.Content {
		if m.Kind != yaml.MappingNode {
			continue
		}
		typeNode := mapGet(m, "type")
		if typeNode == nil || typeNode.Value != "Resource" {
			continue
		}
		resource := mapGet(m, "resource")
		if resource == nil {
			continue
		}
		name := mapGet(resource, "name")
		if name == nil || name.Value != "cpu" {
			continue
		}
		target := mapGet(resource, "target")
		if target == nil {
			continue
		}
		setScalar(target, "averageUtilization", fmt.Sprintf("%d", reco.TargetCPUPct))
	}
}

func documentRoot(doc *yaml.Node) *yaml.Node {
	if doc.Kind == yaml.DocumentNode && len(doc.Content) > 0 {
		return doc.Content[0]
	}
	return doc
}

func mapGet(m *yaml.Node, key string) *yaml.Node {
	if m == nil || m.Kind != yaml.MappingNode {
		return nil
	}
	for i := 0; i+1 < len(m.Content); i += 2 {
		if m.Content[i].Value == key {
			return m.Content[i+1]
		}
	}
	return nil
}

func setScalar(m *yaml.Node, key, value string) {
	if m == nil || m.Kind != yaml.MappingNode {
		return
	}
	for i := 0; i+1 < len(m.Content); i += 2 {
		if m.Content[i].Value == key {
			m.Content[i+1].Kind = yaml.ScalarNode
			m.Content[i+1].Tag = "!!int"
			m.Content[i+1].Value = value
			return
		}
	}
	m.Content = append(m.Content,
		&yaml.Node{Kind: yaml.ScalarNode, Value: key},
		&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!int", Value: value},
	)
}
