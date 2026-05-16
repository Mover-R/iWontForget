package temporal

import (
	"os"
	"strings"
)

type TemporalConfig struct {
	Address   string            `yaml:"address"`
	Namespace string            `yaml:"namespace"`
	Queues    map[string]string `yaml:"queues"`
}

func NewTemporalConfig() *TemporalConfig {
	t := &TemporalConfig{
		Queues: make(map[string]string),
	}
	t.doEnvironment()
	return t
}

func (t *TemporalConfig) doEnvironment() {
	t.Address = os.Getenv("TEMPORAL_ADDRESS")
	t.Namespace = os.Getenv("TEMPORAL_NAMESPACE")

	queueEnv := os.Getenv("TEMPORAL_QUEUES")
	if queueEnv != "" {
		if t.Queues == nil {
			t.Queues = make(map[string]string)
		}
		queues := strings.Split(queueEnv, ",")
		for _, q := range queues {
			parts := strings.Split(strings.TrimSpace(q), ":")
			if len(parts) == 2 {
				t.Queues[strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
			} else if len(parts) == 1 {
				t.Queues["default"] = strings.TrimSpace(parts[0])
			}
		}
	}
}
