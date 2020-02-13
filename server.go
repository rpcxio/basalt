package basalt

import (
	"io"
	"log"
	"net"

	"github.com/smallnest/rpcx/protocol"
	"github.com/smallnest/rpcx/server"
	"github.com/soheilhy/cmux"
	"github.com/tidwall/redcon"
)

// Server is the bitmap server that supports multiple services.
type Server struct {
	addr    string
	bitmaps *Bitmaps
	ln      net.Listener

	rpcxOptions []ConfigRpcxOption
}

// NewServer returns a server.
func NewServer(addr string, bitmaps *Bitmaps, rpcxOptions []ConfigRpcxOption) *Server {
	return &Server{
		addr:        addr,
		bitmaps:     bitmaps,
		rpcxOptions: rpcxOptions,
	}
}

// Serve serves basalt services.
func (s *Server) Serve() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}

	return s.configListener(ln)
}

// Close closes this server.
func (s *Server) Close() error {
	if s.ln == nil {
		return nil
	}

	return s.ln.Close()
}

func (s *Server) configListener(ln net.Listener) error {
	m := cmux.New(ln)

	// rpcx
	rpcxLn := m.Match(rpcxPrefixByteMatcher())

	// admin http
	httpLn := m.Match(cmux.HTTP1Fast())

	// redis
	redisLn := m.Match(cmux.Any())

	go s.startRpcxService(rpcxLn)
	go s.startHTTPService(httpLn)
	go s.startRedisService(redisLn)

	return m.Serve()
}

func (s *Server) startRpcxService(ln net.Listener) {
	srv := server.NewServer()

	for _, opt := range s.rpcxOptions {
		opt(s, srv)
	}

	srv.RegisterName("Bitmap", &RpcxBitmapService{bitmaps: s.bitmaps}, "")
	if err := srv.ServeListener("tcp", ln); err != nil {
		log.Fatalf("failed to start rpcx services: %v", err)
	}
}

// if not config adminAddr, we don't start admin service.
// It is useful for security purpose.
func (s *Server) startHTTPService(ln net.Listener) {
	hs := &HTTPService{
		bitmaps: s.bitmaps,
	}

	if err := hs.Serve(ln); err != nil {
		log.Fatalf("failed to start http service: %v", err)
	}
}

func (s *Server) startRedisService(ln net.Listener) {
	redisService := &RedisService{
		bitmaps: s.bitmaps,
	}
	if err := redcon.Serve(ln, redisService.redisHandler, redisService.redisAccept, redisService.redisClose); err != nil {
		log.Fatalf("failed to start redis services: %v", err)
	}
}

func rpcxPrefixByteMatcher() cmux.Matcher {
	magic := protocol.MagicNumber()
	return func(r io.Reader) bool {
		buf := make([]byte, 1)
		n, _ := r.Read(buf)
		return n == 1 && buf[0] == magic
	}
}
