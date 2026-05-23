package grpc

import (
	"fmt"
	"net"

	grpczap "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/recovery"
	"google.golang.org/grpc"

	"pkg/logger"
)

type Server struct {
	server   *grpc.Server
	config   Config
	log      *logger.Logger
	services []ServiceInterface
}

type ServiceInterface interface {
	Register(server *grpc.Server)
}

func NewServer(cfg Config, opts ...Option) *Server {
	s := &Server{
		config: cfg,
	}
	for _, op := range opts {
		op(s)
	}

	grpcOpts := []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(
			recovery.UnaryServerInterceptor(),           // panic → error
			grpczap.UnaryServerInterceptor(s.log.Zap()), // логирование
		),
		grpc.ChainStreamInterceptor(
			recovery.StreamServerInterceptor(),
			grpczap.StreamServerInterceptor(s.log.Zap()),
		),
	}
	s.server = grpc.NewServer(grpcOpts...)

	for _, service := range s.services {
		service.Register(s.server)
	}

	return s
}

func (s *Server) Run() error {
	addr := fmt.Sprintf("%s:%d", s.config.Host, s.config.Port)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("grpc listen %s: %w", addr, err)
	}
	if s.log != nil {
		s.log.Zap().Sugar().Infof("gRPC server listening on %s", addr)
	}
	return s.server.Serve(lis)
}

func (s *Server) Stop() {
	s.server.GracefulStop()
}
