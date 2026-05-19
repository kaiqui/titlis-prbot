package patch

import (
	"bytes"
	"fmt"

	"gopkg.in/yaml.v3"
)

type hpaShape struct {
	MinReplicas int
	MaxReplicas int
	TargetCPU   int
}

// ValidateNeverReduce ensures the new YAML does not reduce min/max replicas or
// target CPU compared to the current YAML. Reducing means: new < current.
func ValidateNeverReduce(currentYAML, newYAML []byte) error {
	cur, curOK := extractHPA(currentYAML)
	nu, nuOK := extractHPA(newYAML)
	if !curOK || !nuOK {
		return fmt.Errorf("could not extract HPA from one of the documents")
	}
	if nu.MinReplicas < cur.MinReplicas {
		return fmt.Errorf("never-reduce: minReplicas dropped from %d to %d", cur.MinReplicas, nu.MinReplicas)
	}
	if nu.MaxReplicas < cur.MaxReplicas {
		return fmt.Errorf("never-reduce: maxReplicas dropped from %d to %d", cur.MaxReplicas, nu.MaxReplicas)
	}
	// target CPU: lower target means scale earlier (more aggressive); we treat
	// drops below 30 as suspicious but >= 30 is allowed.
	if nu.TargetCPU > 0 && nu.TargetCPU < 30 {
		return fmt.Errorf("never-reduce: targetCPU below safe floor (got %d)", nu.TargetCPU)
	}
	return nil
}

func extractHPA(input []byte) (hpaShape, bool) {
	dec := yaml.NewDecoder(bytes.NewReader(input))
	for {
		var n yaml.Node
		if err := dec.Decode(&n); err != nil {
			break
		}
		root := documentRoot(&n)
		if root == nil || root.Kind != yaml.MappingNode {
			continue
		}
		kind := mapGet(root, "kind")
		if kind == nil || kind.Value != "HorizontalPodAutoscaler" {
			continue
		}
		spec := mapGet(root, "spec")
		if spec == nil {
			continue
		}
		h := hpaShape{}
		if v := mapGet(spec, "minReplicas"); v != nil {
			fmt.Sscanf(v.Value, "%d", &h.MinReplicas)
		}
		if v := mapGet(spec, "maxReplicas"); v != nil {
			fmt.Sscanf(v.Value, "%d", &h.MaxReplicas)
		}
		if metrics := mapGet(spec, "metrics"); metrics != nil {
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
				if target := mapGet(resource, "target"); target != nil {
					if avg := mapGet(target, "averageUtilization"); avg != nil {
						fmt.Sscanf(avg.Value, "%d", &h.TargetCPU)
					}
				}
			}
		}
		return h, true
	}
	return hpaShape{}, false
}
