package server

type Config struct {
	Host    string `yaml:"host"`
	Port    int    `yaml:"port"`
	WithLog bool   `yaml:"with_log"`
}
