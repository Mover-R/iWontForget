package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

func LoadConfigFromFile(path string, out any) error {
	c, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("Failed to config load from file: %v", err)
	}
	if err := yaml.Unmarshal(c, out); err != nil {
		return fmt.Errorf("Failed to config unmarshal: %v", err)
	}

	return nil
}
