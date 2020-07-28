package basalt

import (
	"math/rand"
	"testing"
	"time"

	"github.com/RoaringBitmap/roaring"
)

func TestBitmap_Stats(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	var bitmap = roaring.NewBitmap()
	total := uint32(10 * 10000 * 10000)
	for i := uint32(0); i < total; i++ {
		if rand.Int()%2 == 0 {
			bitmap.Add(i)
		}
	}

	t.Logf("cardinality: %d", bitmap.GetCardinality())
	t.Logf("stats: %+v", bitmap.Stats())

	start := time.Now()
	data, err := bitmap.MarshalBinary()
	t.Logf("marshal took: %d ms", time.Since(start).Milliseconds())
	if err != nil {
		t.Error(err)
	}
	t.Logf("serialized size: %d MB, read bytes: %d MB", bitmap.GetSerializedSizeInBytes()/(1024*1024), len(data)/(1024*1024))

	start = time.Now()
	persistedBitmap := bitmap.Clone()
	t.Logf("clone took: %d ms", time.Since(start).Milliseconds())

	persistedBitmap.Clear()

	if bitmap.GetCardinality() < uint64(total/4) {
		t.Errorf("not real clone")
	}
}
