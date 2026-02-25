package rest

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"go.uber.org/zap"
)

type routerHandler interface {
	GetHandler(eh *EndpointHandler) http.Handler
}

type Server struct {
	logger          *zap.SugaredLogger
	shutdownTimeout time.Duration
	client          *http.Server
	listener        net.Listener
	isReady         bool
	EndpointHandler *EndpointHandler
	certFile        string
	keyFile         string
}

func NewServer(port int, shutdownTimeout time.Duration,
	routerHandler routerHandler, logger *zap.SugaredLogger,
	endpointHandler *EndpointHandler, certFile string, keyFile string) (*Server, error) {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return nil, fmt.Errorf("cannot bind HTTP server '%d': %v", port, err)
	}
	return &Server{
		client: &http.Server{
			Handler: routerHandler.GetHandler(endpointHandler),
		},
		listener:        listener,
		logger:          logger,
		shutdownTimeout: shutdownTimeout,
		isReady:         false,
		certFile:        certFile,
		keyFile:         keyFile,
	}, nil
}

func (s *Server) Ready() error {
	if s.isReady {
		return nil
	}

	return errors.New("server is not ready")
}

func (s *Server) Stop() error {
	s.isReady = false
	s.logger.Infof("[%s] HTTP server is stoping...", s.listener.Addr().String())

	ctx, cancel := context.WithTimeout(context.Background(), s.shutdownTimeout)
	defer cancel()

	s.client.SetKeepAlivesEnabled(false)

	if err := s.client.Shutdown(ctx); err != nil {
		return fmt.Errorf("cannot stop HTTP server: %w", err)
	}
	s.logger.Infof("[%s] HTTP server was stopped", s.listener.Addr().String())

	return nil
}

func (s *Server) Run() {
	protocol := "HTTP"
	if s.certFile != "" && s.keyFile != "" {
		protocol = "HTTPS"
	}
	s.logger.Infof("[%s] %s server is starting...", s.listener.Addr().String(), protocol)

	go func() {
		s.isReady = true
		var err error
		if s.certFile != "" && s.keyFile != "" {
			s.logger.Info("certFILES: ", s.certFile)
			s.logger.Info("keyFILES: ", s.keyFile)
			err = s.client.ServeTLS(s.listener, s.certFile, s.keyFile)
		} else {
			err = s.client.Serve(s.listener)
		}

		if err != nil {
			s.isReady = false
			if errors.Is(err, http.ErrServerClosed) {
				return
			}

			s.logger.Errorf("[%s] %s server stopped with error: %s", s.listener.Addr().String(), protocol, err)
		}
	}()
}
