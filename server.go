package basalt

import (
	"bufio"
	"errors"
	"io"
	"log"
	"net"
	"os"

	"github.com/smallnest/rpcx/protocol"
	"github.com/smallnest/rpcx/server"
	"github.com/soheilhy/cmux"
	"github.com/tidwall/redcon"
)

// Errors for bitmaps
var (
	ErrPersistFileNotFound = errors.New("persist file not found")
)

// Server is the bitmap server that supports multiple services.
type Server struct {
	addr    string
	bitmaps *Bitmaps
	ln      net.Listener

	rpcxOptions []ConfigRpcxOption

	persistFile string
}

// NewServer returns a server.
func NewServer(addr string, bitmaps *Bitmaps, rpcxOptions []ConfigRpcxOption, persistFile string) *Server {
	return &Server{
		addr:        addr,
		bitmaps:     bitmaps,
		rpcxOptions: rpcxOptions,
		persistFile: persistFile,
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

	srv.RegisterName("Bitmap", &RpcxBitmapService{s: s}, "")
	if err := srv.ServeListener("tcp", ln); err != nil {
		log.Fatalf("failed to start rpcx services: %v", err)
	}
}

// if not config adminAddr, we don't start admin service.
// It is useful for security purpose.
func (s *Server) startHTTPService(ln net.Listener) {
	hs := &HTTPService{
		s: s,
	}

	if err := hs.Serve(ln); err != nil {
		log.Fatalf("failed to start http service: %v", err)
	}
}

func (s *Server) startRedisService(ln net.Listener) {
	redisService := &RedisService{
		s: s,
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

// Save saves the data into file.
func (s *Server) Save() error {
	if s.persistFile == "" {
		return ErrPersistFileNotFound
	}
	file, err := os.Create(s.persistFile)
	if err != nil {
		return err
	}

	w := bufio.NewWriter(file)
	err = s.bitmaps.Save(w)
	if err != nil {
		file.Close()
		return err
	}
	err = w.Flush()
	if err != nil {
		return err
	}

	return file.Close()
}

// Restore retores the data from file.
func (s *Server) Restore() error {
	if s.persistFile == "" {
		return ErrPersistFileNotFound
	}

	file, err := os.Open(s.persistFile)
	if err != nil {
		return err
	}

	r := bufio.NewReader(file)
	err = s.bitmaps.Read(r)
	if err != nil {
		return err
	}

	return file.Close()
}

func (s *Server) add(name, value string, callback bool) error {
	v, err := str2uint32(value)
	if err != nil {
		return err
	}

	s.bitmaps.Add(name, v, callback)
	return nil
}

func (s *Server) addMany(name, values string, callback bool) error {
	vs, err := str2uint32s(values)
	if err != nil {
		return err
	}

	s.bitmaps.AddMany(name, vs, callback)
	return nil
}

func (s *Server) remove(name, value string, callback bool) error {
	v, err := str2uint32(value)
	if err != nil {
		return err
	}

	s.bitmaps.Remove(name, v, callback)
	return err
}

func (s *Server) drop(name string, callback bool) {
	s.bitmaps.RemoveBitmap(name, callback)
}

func (s *Server) clear(name string, callback bool) {
	s.bitmaps.ClearBitmap(name, callback)
}
