package logger

import (
	"cmp"
	"log"
)

type LogOption func(l *Logger)

func WithName(name string) LogOption {
	return func(l *Logger) {
		l.Name = cmp.Or(name, string(defaultName))
	}
}

func WithLevel(level log.Level) LogOption {
	return func(l *Logger) {
		l.Level = level
	}
}

func WithConsoleOption(useConsole bool) LogOption {
	return func(l *Logger) {
		l.UseConsole = useConsole
	}
}
