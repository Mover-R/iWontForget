package logger

type Option func(l *Config)

func WithName(name string) Option {
	return func(l *Config) {
		l.Name = name
	}
}

func WithLevel(level string) Option {
	return func(l *Config) {
		l.Level = level
	}
}

func WithDevelopment(development bool) Option {
	return func(l *Config) {
		l.Development = development
	}
}
