package basalt

import (
	"sync"

	"github.com/RoaringBitmap/roaring"
)

// Bitmaps contains all bitmaps of namespace.
type Bitmaps struct {
	mu      sync.RWMutex
	bitmaps map[string]*Bitmap
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
func (bs *Bitmaps) Add(name string, v uint32) {
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
func (bs *Bitmaps) AddMany(name string, v []uint32) {
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
func (bs *Bitmaps) Remove(name string, v uint32) {
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
func (bs *Bitmaps) RemoveBitmap(name string) {
	bs.mu.Lock()
	delete(bs.bitmaps, name)
	bs.mu.Unlock()
}

// ClearBitmap clear a bitmap.
func (bs *Bitmaps) ClearBitmap(name string) {
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
