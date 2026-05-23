package server

import (
	"pkg/logger"
)

type Option func(s *Server)

func WithLogger(log *logger.Logger) Option {
	return func(s *Server) {
		s.log = log
	}
}

func WithConfig(config Config) Option {
	return func(s *Server) {
		s.config = config
	}
}

func WithService(service HTTPServiceInterface) Option {
	return func(s *Server) {
		s.service = append(s.service, service)
	}
}

func WithServices(service ...HTTPServiceInterface) Option {
	return func(s *Server) {
		s.service = append(s.service, service...)
	}
}
