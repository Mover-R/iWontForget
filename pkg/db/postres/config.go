package postres

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	defaultPGHost     = "localhost"
	defaultPGPort     = 5432
	defaultPGDB       = "postgres"
	defaultPGUser     = "postgres"
	defaultPGSSLMode  = "disable"
	defaultPGPassword = "123"

	defaultPGMinConns = 2
	defaultPGMaxConns = 10
)

type PostgresConfig struct {
	DSN             string        `yaml:"dsn"`
	Host            string        `yaml:"host"`
	Port            int           `yaml:"port"`
	Database        string        `yaml:"database"`
	User            string        `yaml:"user"`
	Password        string        `yaml:"password"`
	SSLMode         string        `yaml:"sslmode"`
	MaxConns        int           `yaml:"max_conns"`
	MinConns        int           `yaml:"min_conns"`
	ConnMaxIdleTime time.Duration `yaml:"conn_max_idle_time"`
	ConnMaxLifetime time.Duration `yaml:"conn_max_lifetime"`
}

func NewPostgresConfigFromFile(filePath string) (*PostgresConfig, error) {
	var c PostgresConfig
	rawConfig, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("read config file: %w", err)
	}

	err = yaml.Unmarshal(rawConfig, &c)
	if err != nil {
		return nil, fmt.Errorf("unmarshal config: %w", err)
	}
	if err := c.DoEnv(); err != nil {
		return nil, fmt.Errorf("NewPostgresConfigFromFile: %w", err)
	}
	return &c, nil
}

func (c *PostgresConfig) DoEnv() error {
	if host := os.Getenv("POSTGRES_HOST"); host != "" {
		c.Host = host
	}
	if portStr := os.Getenv("POSTGRES_PORT"); portStr != "" {
		port, err := strconv.Atoi(portStr)
		if err != nil {
			return fmt.Errorf("config.doEnv: Failed to parse POSTGRES_PORT: %w", err)
		}
		c.Port = port
	}
	if db := os.Getenv("POSTGRES_DB"); db != "" {
		c.Database = db
	}
	if user := os.Getenv("POSTGRES_USER"); user != "" {
		c.User = user
	}
	if password := os.Getenv("POSTGRES_PASSWORD"); password != "" {
		c.Password = password
	}
	if sslMode := os.Getenv("POSTGRES_SSLMODE"); sslMode != "" {
		c.SSLMode = sslMode
	}
	if maxConnsStr := os.Getenv("POSTGRES_MAX_CONNS"); maxConnsStr != "" {
		maxConns, err := strconv.Atoi(maxConnsStr)
		if err != nil {
			return fmt.Errorf("config.doEnv: Failed to parse POSTGRES_MAX_CONNS: %w", err)
		}
		c.MaxConns = maxConns
	}
	if minConnsStr := os.Getenv("POSTGRES_MIN_CONNS"); minConnsStr != "" {
		minConns, err := strconv.Atoi(minConnsStr)
		if err != nil {
			return fmt.Errorf("config.doEnv: Failed to parse POSTGRES_MIN_CONNS: %w", err)
		}
		c.MinConns = minConns
	}
	return nil
}

func (c *PostgresConfig) GetHost() string {
	if c.Host == "" {
		return defaultPGHost
	}
	return c.Host
}

func (c *PostgresConfig) GetPort() int {
	if c.Port == 0 {
		return defaultPGPort
	}
	return c.Port
}

func (c *PostgresConfig) GetDatabase() string {
	if c.Database == "" {
		return defaultPGDB
	}
	return c.Database
}

func (c *PostgresConfig) GetUser() string {
	if c.User == "" {
		return defaultPGUser
	}
	return c.User
}

func (c *PostgresConfig) GetPassword() string {
	if c.Password == "" {
		return defaultPGPassword
	}
	return c.Password
}

func (c *PostgresConfig) GetSSLMode() string {
	if c.SSLMode == "" {
		return defaultPGSSLMode
	}
	return c.SSLMode
}

func (c *PostgresConfig) GetMaxConns() int {
	if c.MaxConns == 0 {
		return defaultPGMaxConns
	}
	return c.MaxConns
}

func (c *PostgresConfig) GetDSN() string {
	if c.DSN != "" {
		return c.DSN
	}

	host := c.GetHost()
	port := c.GetPort()
	dbname := c.GetDatabase()
	user := c.GetUser()
	password := c.GetPassword()
	dsn := fmt.Sprintf(
		"host=%s port=%d dbname=%s user=%s password=%s target_session_attrs=read-write",
		host, port, dbname, user, password)

	return dsn
}
