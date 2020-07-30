package basalt

import (
	"encoding/binary"
	"fmt"
	"io"
	"sync"

	"github.com/RoaringBitmap/roaring"
	"github.com/smallnest/log"
)

// OP bitmaps operations
type OP byte

const (
	BmOpAdd     OP = 1
	BmOpAddMany    = 2
	BmOpRemove     = 3
	BmOpDrop       = 4
	BmOpClear      = 5
)

// Bitmaps contains all bitmaps of namespace.
type Bitmaps struct {
	mu            sync.RWMutex
	bitmaps       map[string]*Bitmap
	writeCallback func(op OP, value string)
}

// NewBitmaps creates a Bitmaps.
func NewBitmaps() *Bitmaps {
	return &Bitmaps{
		bitmaps: make(map[string]*Bitmap),
	}
}

// Bitmap is the goroutine-safe bitmap.
type Bitmap struct {
	mu     sync.RWMutex
	bitmap *roaring.Bitmap
}

// Add adds a value.
func (bs *Bitmaps) Add(name string, v uint32, callback bool) {
	if bs.writeCallback != nil && callback {
		bs.writeCallback(BmOpAdd, fmt.Sprintf("%s,%d", name, v))
		return
	}

	bs.mu.Lock()
	bm := bs.bitmaps[name]
	if bm == nil {
		bm = &Bitmap{
			bitmap: roaring.NewBitmap(),
		}
		bs.bitmaps[name] = bm
	}
	bs.mu.Unlock()

	bm.mu.Lock()
	bm.bitmap.Add(v)
	bm.mu.Unlock()
}

// AddMany adds multiple values.
func (bs *Bitmaps) AddMany(name string, v []uint32, callback bool) {
	if bs.writeCallback != nil && callback {
		bs.writeCallback(BmOpAddMany, fmt.Sprintf("%s,%d", name, ints2str(v)))
		return
	}

	bs.mu.Lock()
	bm := bs.bitmaps[name]
	if bm == nil {
		bm = &Bitmap{
			bitmap: roaring.NewBitmap(),
		}
		bs.bitmaps[name] = bm
	}
	bs.mu.Unlock()

	bm.mu.Lock()
	bm.bitmap.AddMany(v)
	bm.mu.Unlock()
}

// Remove removes a value.
func (bs *Bitmaps) Remove(name string, v uint32, callback bool) {
	if bs.writeCallback != nil && callback {
		bs.writeCallback(BmOpRemove, fmt.Sprintf("%s,%d", name, v))
		return
	}

	bs.mu.Lock()
	bm := bs.bitmaps[name]
	if bm == nil {
		bm = &Bitmap{
			bitmap: roaring.NewBitmap(),
		}
		bs.bitmaps[name] = bm
	}
	bs.mu.Unlock()

	bm.mu.Lock()
	bm.bitmap.Remove(v)
	bm.mu.Unlock()
}

// RemoveBitmap removes a bitmap.
func (bs *Bitmaps) RemoveBitmap(name string, callback bool) {
	if bs.writeCallback != nil && callback {
		bs.writeCallback(BmOpDrop, name)
		return
	}

	bs.mu.Lock()
	delete(bs.bitmaps, name)
	bs.mu.Unlock()
}

// ClearBitmap clear a bitmap.
func (bs *Bitmaps) ClearBitmap(name string, callback bool) {
	if bs.writeCallback != nil && callback {
		bs.writeCallback(BmOpClear, name)
		return
	}

	bs.mu.RLock()
	bm := bs.bitmaps[name]
	if bm == nil {
		bs.mu.RUnlock()
		return
	}
	bs.mu.RUnlock()
	bm.bitmap.Clear()
}

// Exists checks whether a value exists.
func (bs *Bitmaps) Exists(name string, v uint32) bool {
	bs.mu.RLock()
	bm := bs.bitmaps[name]
	if bm == nil {
		bs.mu.RUnlock()
		return false
	}
	bs.mu.RUnlock()

	bm.mu.RLock()
	existed := bm.bitmap.Contains(v)
	bm.mu.RUnlock()

	return existed
}

// Card returns the number of integers contained in the bitmap.
func (bs *Bitmaps) Card(name string) uint64 {
	bs.mu.RLock()
	bm := bs.bitmaps[name]
	if bm == nil {
		bs.mu.RUnlock()
		return 0
	}
	bs.mu.RUnlock()

	bm.mu.RLock()
	num := bm.bitmap.GetCardinality()
	bm.mu.RUnlock()

	return num
}

type Stats struct {
	Cardinality uint64
	Containers  uint64

	ArrayContainers      uint64
	ArrayContainerBytes  uint64
	ArrayContainerValues uint64

	BitmapContainers      uint64
	BitmapContainerBytes  uint64
	BitmapContainerValues uint64

	RunContainers      uint64
	RunContainerBytes  uint64
	RunContainerValues uint64
}

// Stats gets the stats of named bitmap.
func (bs *Bitmaps) Stats(name string) Stats {
	bs.mu.RLock()
	bm := bs.bitmaps[name]
	if bm == nil {
		bs.mu.RUnlock()
		return Stats{}
	}
	bs.mu.RUnlock()

	bm.mu.RLock()
	stats := bm.bitmap.Stats()
	bm.mu.RUnlock()

	return Stats(stats)
}

func (bs *Bitmaps) intersection(names ...string) *roaring.Bitmap {
	var bms []*roaring.Bitmap

	bs.mu.RLock()
	for _, name := range names {
		bm := bs.bitmaps[name]
		if bm == nil {
			bs.mu.RUnlock()
			return nil
		}
		bms = append(bms, bm.bitmap)

	}
	bs.mu.RUnlock()

	return roaring.ParAnd(0, bms...)
}

// Inter computes the intersection (AND) of all provided bitmaps.
func (bs *Bitmaps) Inter(names ...string) []uint32 {
	bm := bs.intersection(names...)
	if bm == nil {
		return nil
	}
	return bm.ToArray()
}

// InterStore computes the intersection (AND) of all provided bitmaps and save to destination.
func (bs *Bitmaps) InterStore(destination string, names ...string) uint64 {
	bm := bs.intersection(names...)
	if bm == nil {
		return 0
	}

	bs.mu.Lock()
	bs.bitmaps[destination] = &Bitmap{bitmap: bm}
	bs.mu.Unlock()

	return bm.GetCardinality()
}

func (bs *Bitmaps) union(names ...string) *roaring.Bitmap {
	var bms []*roaring.Bitmap

	bs.mu.RLock()
	for _, name := range names {
		bm := bs.bitmaps[name]
		if bm != nil {
			bms = append(bms, bm.bitmap)
		}
	}
	bs.mu.RUnlock()

	return roaring.ParHeapOr(0, bms...)
}

// Union computes the union (OR) of all provided bitmaps.
func (bs *Bitmaps) Union(names ...string) []uint32 {
	bm := bs.union(names...)
	return bm.ToArray()
}

// UnionStore computes the union (OR) of all provided bitmaps and store to destination.
func (bs *Bitmaps) UnionStore(destination string, names ...string) uint64 {
	bm := bs.union(names...)

	bs.mu.Lock()
	bs.bitmaps[destination] = &Bitmap{bitmap: bm}
	bs.mu.Unlock()

	return bm.GetCardinality()
}

func (bs *Bitmaps) xor(name1, name2 string) *roaring.Bitmap {
	var rbm1, rbm2 *roaring.Bitmap

	bs.mu.RLock()
	bm1 := bs.bitmaps[name1]
	if bm1 == nil {
		rbm1 = roaring.NewBitmap()
	} else {
		rbm1 = bm1.bitmap
	}
	bm2 := bs.bitmaps[name2]
	if bm2 == nil {
		rbm2 = roaring.NewBitmap()
	} else {
		rbm2 = bm2.bitmap
	}
	bs.mu.RUnlock()

	return roaring.Xor(rbm1, rbm2)
}

// Xor computes the symmetric difference between two bitmaps and returns the result
func (bs *Bitmaps) Xor(name1, name2 string) []uint32 {
	bm := bs.xor(name1, name2)
	return bm.ToArray()
}

// XorStore computes the symmetric difference between two bitmaps and save the result to destination.
func (bs *Bitmaps) XorStore(destination, name1, name2 string) uint64 {
	bm := bs.xor(name1, name2)

	bs.mu.Lock()
	bs.bitmaps[destination] = &Bitmap{bitmap: bm}
	bs.mu.Unlock()

	return bm.GetCardinality()
}

func (bs *Bitmaps) diff(name1, name2 string) *roaring.Bitmap {
	var rbm1, rbm2 *roaring.Bitmap

	bs.mu.RLock()
	bm1 := bs.bitmaps[name1]
	if bm1 == nil {
		rbm1 = roaring.NewBitmap()
	} else {
		rbm1 = bm1.bitmap
	}
	bm2 := bs.bitmaps[name2]
	if bm2 == nil {
		rbm2 = roaring.NewBitmap()
	} else {
		rbm2 = bm2.bitmap
	}
	bs.mu.RUnlock()

	return roaring.AndNot(rbm1, rbm2)
}

// Diff computes the difference between two bitmaps and returns the result.
func (bs *Bitmaps) Diff(name1, name2 string) []uint32 {
	bm := bs.diff(name1, name2)
	return bm.ToArray()
}

// DiffStore computes the difference between two bitmaps and save the result to destination.
func (bs *Bitmaps) DiffStore(destination, name1, name2 string) uint64 {
	bm := bs.diff(name1, name2)

	bs.mu.Lock()
	bs.bitmaps[destination] = &Bitmap{bitmap: bm}
	bs.mu.Unlock()

	return bm.GetCardinality()
}

// Save saves bitmaps to the io.Writer.
func (bs *Bitmaps) Save(w io.Writer) error {
	var keys []string
	bs.mu.RLock()
	for k := range bs.bitmaps {
		keys = append(keys, k)
	}
	bs.mu.RUnlock()

	for _, k := range keys {
		bs.mu.RLock()
		bm := bs.bitmaps[k]
		bs.mu.RUnlock()
		if bm != nil {
			if err := bs.saveBitmap(w, k, bm.bitmap); err != nil {
				return err
			}
		}
	}

	return nil
}

func (bs *Bitmaps) saveBitmap(w io.Writer, name string, bm *roaring.Bitmap) error {
	if bm == nil {
		return nil
	}

	err := binary.Write(w, binary.LittleEndian, uint32(len(name)))
	if err != nil {
		log.Errorf("failed to write len of name %s: %v", name, err)
		return err
	}
	_, err = w.Write([]byte(name))
	if err != nil {
		log.Errorf("failed to write name %s: %v", name, err)
		return err
	}

	bs.mu.RLock()
	pBitmap := bm.Clone()
	bs.mu.RUnlock()
	_, err = pBitmap.WriteTo(w)
	if err != nil {
		log.Errorf("failed to write bitmap %s: %v", name, err)
		return err
	}

	return nil
}

// Read restores bitmaps from a io.Reader.
func (bs *Bitmaps) Read(r io.Reader) error {
	for {
		name, bm, err := readBitmap(r)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		b := &Bitmap{
			bitmap: bm,
		}

		bs.mu.Lock()
		bs.bitmaps[name] = b
		bs.mu.Unlock()

	}
}

func readBitmap(r io.Reader) (name string, bm *roaring.Bitmap, err error) {
	var l uint32
	err = binary.Read(r, binary.LittleEndian, &l)
	if err != nil {
		if err == io.EOF {
			return "", nil, err
		}
		log.Errorf("failed to read len of name: %v", err)
		return "", nil, err
	}

	var data = make([]byte, int(l))
	_, err = io.ReadFull(r, data)
	if err != nil {
		log.Errorf("failed to read name: %v", err)
		return "", nil, err
	}
	name = string(data)

	bm = roaring.NewBitmap()
	_, err = bm.ReadFrom(r)
	if err != nil {
		log.Errorf("failed to read name %s: %v", name, err)
		return "", nil, err
	}

	return name, bm, nil
}
