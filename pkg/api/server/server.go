package server

import (
	"github.com/go-chi/chi/middleware"
	"github.com/go-chi/chi/v5"

	"pkg/logger"
)

type Server struct {
	router chi.Router
	config Config
	log    *logger.Logger

	service []HTTPServiceInterface
}

type HTTPServiceInterface interface {
	BindToServer(router chi.Router)
	GetEndpoints() []Endpoint
}

type Endpoint struct {
	Method string
	Path   string
}

func NewServer(config Config) *Server {
	return &Server{
		router: chi.NewRouter(),
		config: config,
	}
}

func (s *Server) Init(opts ...Option) {
	for _, opt := range opts {
		opt(s)
	}

	s.router.Use(middleware.Recoverer)
	s.router.Use(middleware.RequestID)

	for _, service := range s.service {
		service.BindToServer(s.router)
	}
}

func (s Server) CollectEndpoints() []Endpoint {
	var endpoints []Endpoint
	for _, service := range s.service {
		endpoints = append(endpoints, service.GetEndpoints()...)
	}
	return endpoints
}
