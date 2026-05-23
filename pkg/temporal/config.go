package temporal

import (
	"os"
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
}
