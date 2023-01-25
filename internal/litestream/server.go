package litestream

import (
	"fmt"
	"os"
	"time"

	"github.com/benbjohnson/litestream"

	"github.com/temporalio/temporalite/internal/litestream/config"
)

type BackupServer interface {
	Start() error
	Stop()
}

type Server struct {
	server *litestream.Server
	dbPath string
	config config.Config
}

func NewServer(config config.Config, dbPath string) (*Server, error) {
	liteServer := litestream.NewServer()
	if err := liteServer.Open(); err != nil {
		return nil, fmt.Errorf("open server: %w", err)
	}

	server := &Server{
		server: liteServer,
		dbPath: dbPath,
		config: config,
	}
	return server, nil
}
func (s *Server) Start() error {
	err := s.waitForDB()
	if err != nil {
		return err
	}

	err = s.server.Watch(s.dbPath, func(path string) (*litestream.DB, error) {
		// TODO[litestream]: Validate configuration.
		return config.NewDBFromConfigWithPath(s.config.DBs[0], s.dbPath)
	})
	if err != nil {
		return err
	}

	return nil
}

func (s *Server) Stop() {
	fmt.Println("Stopping litestream server...")
	_ = s.server.Close()
}

// TODO[litestream]: Check this code.
func (s *Server) waitForDB() error {
	n := 1
	for n < 10 {
		_, err := os.Stat(s.dbPath)

		// check if error is "file not exists"
		if os.IsNotExist(err) {
			fmt.Printf("%v file does not exist\n", s.dbPath)
			time.Sleep(1 * time.Second)
		} else {
			return nil
		}
		n += 1
	}
	return nil
}

func NewNoopServer() noopServer {
	return noopServer{}
}

type noopServer struct{}

func (noopServer) Start() error {
	return nil
}

func (noopServer) Stop() {}
