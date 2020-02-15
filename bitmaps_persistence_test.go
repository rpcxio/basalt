package basalt

import (
	"bytes"
	"math/rand"
	"testing"
)

func TestBitmaps_Persistence(t *testing.T) {
	var buf = bytes.NewBuffer(nil)

	bms := NewBitmaps()

	var values1 []uint32
	for i := 0; i < 100; i++ {
		v := uint32(rand.Int31())
		values1 = append(values1, v)
		bms.Add("test1", v)
	}
	var values2 []uint32
	for i := 0; i < 100; i++ {
		v := uint32(rand.Int31())
		values2 = append(values2, v)
		bms.Add("test2", v)
	}

	err := bms.Save(buf)
	if err != nil {
		t.Fatalf("failed to save Bitmaps: %v", err)
	}

	// read

	bms = NewBitmaps()
	err = bms.Read(buf)
	if err != nil {
		t.Fatalf("failed to restore Bitmaps: %v", err)
	}

	for i := 0; i < 100; i++ {
		if !bms.Exists("test1", values1[i]) {
			t.Fatalf("not found %d in retored bitmap test1", values1[i])
		}
	}
	for i := 0; i < 100; i++ {
		if !bms.Exists("test2", values2[i]) {
			t.Fatalf("not found %d in retored bitmap test2", values2[i])
		}
	}
}
