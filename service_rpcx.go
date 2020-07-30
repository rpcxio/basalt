package basalt

import (
	"context"

	"github.com/smallnest/rpcx/server"
)

// ConfigRpcxOption defines the rpcx config function.
type ConfigRpcxOption func(*Server, *server.Server)

// RpcxBitmapService provides the rpcx service for Bitmaps.
type RpcxBitmapService struct {
	s                  *Server
	confChangeCallback ConfChange
}

// BitmapValueRequest contains the name of bitmap and value.
type BitmapValueRequest struct {
	Name  string
	Value uint32
}

// BitmapValuesRequest contains the name of bitmap and values.
type BitmapValuesRequest struct {
	Name   string
	Values []uint32
}

// BitmapStoreRequest contains the name of destination and names of bitmaps.
type BitmapStoreRequest struct {
	Destination string
	Names       []string
}

// BitmapPairRequest contains the name of two bitmaps.
type BitmapPairRequest struct {
	Name1 string
	Name2 string
}

// BitmapDstAndPairRequest contains  destination and the name of two bitmaps.
type BitmapDstAndPairRequest struct {
	Destination string
	Name1       string
	Name2       string
}

// Add adds a value in the bitmap with name.
func (s *RpcxBitmapService) Add(ctx context.Context, req *BitmapValueRequest, reply *bool) error {
	s.s.bitmaps.Add(req.Name, req.Value, true)
	*reply = true
	return nil
}

// AddMany adds multiple values in the bitmap with name.
func (s *RpcxBitmapService) AddMany(ctx context.Context, req *BitmapValuesRequest, reply *bool) error {
	s.s.bitmaps.AddMany(req.Name, req.Values, true)
	*reply = true
	return nil
}

// Remove removes a value in the bitmap with name.
func (s *RpcxBitmapService) Remove(ctx context.Context, req *BitmapValueRequest, reply *bool) error {
	s.s.bitmaps.Remove(req.Name, req.Value, true)
	*reply = true
	return nil
}

// RemoveBitmap removes the bitmap.
func (s *RpcxBitmapService) RemoveBitmap(ctx context.Context, name string, reply *bool) error {
	s.s.bitmaps.RemoveBitmap(name, true)
	*reply = true
	return nil
}

// ClearBitmap clears the bitmap and set it to be empty.
func (s *RpcxBitmapService) ClearBitmap(ctx context.Context, name string, reply *bool) error {
	s.s.bitmaps.ClearBitmap(name, true)
	*reply = true
	return nil
}

// Exists checks whether the value exists.
func (s *RpcxBitmapService) Exists(ctx context.Context, req *BitmapValueRequest, reply *bool) error {
	*reply = s.s.bitmaps.Exists(req.Name, req.Value)
	return nil
}

// Card gets number of integers in the bitmap.
func (s *RpcxBitmapService) Card(ctx context.Context, name string, reply *uint64) error {
	*reply = s.s.bitmaps.Card(name)
	return nil
}

// Inter gets the intersection of bitmaps.
func (s *RpcxBitmapService) Inter(ctx context.Context, names []string, reply *[]uint32) error {
	*reply = s.s.bitmaps.Inter(names...)
	return nil
}

// InterStore gets the intersection of bitmaps and stores into destination.
func (s *RpcxBitmapService) InterStore(ctx context.Context, req *BitmapStoreRequest, reply *bool) error {
	s.s.bitmaps.InterStore(req.Destination, req.Names...)
	*reply = true
	return nil
}

// Union gets the union of bitmaps.
func (s *RpcxBitmapService) Union(ctx context.Context, names []string, reply *[]uint32) error {
	*reply = s.s.bitmaps.Union(names...)
	return nil
}

// UnionStore gets the union of bitmaps and stores into destination.
func (s *RpcxBitmapService) UnionStore(ctx context.Context, req *BitmapStoreRequest, reply *bool) error {
	s.s.bitmaps.UnionStore(req.Destination, req.Names...)
	*reply = true
	return nil
}

// Xor gets the symmetric difference between bitmaps.
func (s *RpcxBitmapService) Xor(ctx context.Context, names *BitmapPairRequest, reply *[]uint32) error {
	*reply = s.s.bitmaps.Xor(names.Name1, names.Name2)
	return nil
}

// XorStore gets the symmetric difference between bitmaps and stores into destination.
func (s *RpcxBitmapService) XorStore(ctx context.Context, names *BitmapDstAndPairRequest, reply *bool) error {
	s.s.bitmaps.XorStore(names.Destination, names.Name1, names.Name2)
	*reply = true
	return nil
}

// Diff gets the difference between two bitmaps.
func (s *RpcxBitmapService) Diff(ctx context.Context, names *BitmapPairRequest, reply *[]uint32) error {
	*reply = s.s.bitmaps.Diff(names.Name1, names.Name2)
	return nil
}

// DiffStore gets the difference between two bitmaps and stores into destination.
func (s *RpcxBitmapService) DiffStore(ctx context.Context, names *BitmapDstAndPairRequest, reply *bool) error {
	s.s.bitmaps.DiffStore(names.Destination, names.Name1, names.Name2)
	*reply = true
	return nil
}

// Stats get the stats of bitmap `name`.
func (s *RpcxBitmapService) Stats(ctx context.Context, name string, reply *Stats) error {
	stats := s.s.bitmaps.Stats(name)
	*reply = stats
	return nil
}

// Save persists bitmaps.
func (s *RpcxBitmapService) Save(ctx context.Context, dummy string, reply *bool) error {
	err := s.s.Save()
	if err == nil {
		*reply = true
	}
	return err
}

type AddNodeRequest struct {
	ID   uint64
	Addr string
}

// AddNode adds a raft node.
func (s *RpcxBitmapService) AddNode(ctx context.Context, req *AddNodeRequest, reply *bool) error {
	if s.confChangeCallback != nil {
		s.confChangeCallback.AddNode(req.ID, []byte(req.Addr))
	}

	*reply = true
	return nil
}

// RemoveNode removes a raft node.
func (s *RpcxBitmapService) RemoveNode(ctx context.Context, req uint64, reply *bool) error {
	if s.confChangeCallback != nil {
		s.confChangeCallback.RemoveNode(req)
	}

	*reply = true
	return nil
}
