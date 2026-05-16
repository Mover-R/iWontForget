package logger

import (
	"github.com/go-logr/logr"
	"github.com/go-logr/zapr"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	defaultName  = "app"
	defaultLevel = "info"
)

type Logger struct {
	logr logr.Logger
	zap  *zap.Logger
}

type Config struct {
	Name        string
	Level       string
	Development bool
}

func (l *Logger) Logr() logr.Logger {
	return l.logr
}

func (l *Logger) Zap() *zap.Logger {
	return l.zap
}

func (l *Logger) Sync() error {
	if l.zap == nil {
		return nil
	}
	return l.zap.Sync()
}

func NewLogger(cfg Config) (*Logger, error) {
	if cfg.Name == "" {
		cfg.Name = defaultName
	}
	if cfg.Level == "" {
		cfg.Level = defaultLevel
	}

	zapCfg := zap.NewProductionConfig()
	if cfg.Development {
		zapCfg = zap.NewDevelopmentConfig()
	}

	zapLevel, err := zapcore.ParseLevel(cfg.Level)
	if err != nil {
		return nil, err
	}
	zapCfg.Level = zap.NewAtomicLevelAt(zapLevel)

	zapLogger, err := zapCfg.Build()
	if err != nil {
		return nil, err
	}

	l := &Logger{
		logr: zapr.NewLogger(zapLogger).WithName(cfg.Name),
		zap:  zapLogger,
	}

	return l, nil
}

func NewLoggerWithOptions(opts ...Option) (*Logger, error) {
	cfg := Config{
		Name:  defaultName,
		Level: defaultLevel,
	}

	for _, opt := range opts {
		opt(&cfg)
	}

	return NewLogger(cfg)
}
