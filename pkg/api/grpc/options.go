package grpc

import "pkg/logger"

type Option func(*Server)

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

func WithServices(services ...ServiceInterface) Option {
	return func(s *Server) {
		s.services = append(s.services, services...)
	}
}

func WithService(service ServiceInterface) Option {
	return func(s *Server) {
		s.services = append(s.services, service)
	}
}
