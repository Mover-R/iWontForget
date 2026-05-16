package logger

import (
	"fmt"

	"github.com/go-logr/logr"
	"github.com/go-logr/zapr"

	"a.yandex-team.ru/library/go/core/log"
	"a.yandex-team.ru/library/go/core/log/zap"
)

const defaultName = "http"

type Logger struct {
	Level      log.Level
	Name       string
	UseConsole bool
	LogrLogger logr.Logger
	ZapLogger  *zap.Logger
}

func (l *Logger) GetLogr() logr.Logger {
	return l.LogrLogger
}

func (l *Logger) GetZap() *zap.Logger {
	return l.ZapLogger
}

func GetLogger(options ...LogOption) (*Logger, error) {
	s := &Logger{}
	for _, opt := range options {
		opt(s)
	}
	var err error
	if s.UseConsole {
		s.ZapLogger, err = zap.New(zap.CLIConfig(s.Level))
	} else {
		s.ZapLogger, err = zap.NewDeployLogger(s.Level)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to init zap logger: %w", err)
	}
	s.LogrLogger = zapr.NewLogger(s.ZapLogger.L).WithName(s.Name)
	return s, nil
}
