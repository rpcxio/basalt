package basalt

import (
	"math/rand"
	"testing"
)

func TestBitmaps_Basic(t *testing.T) {
	bms := NewBitmaps()

	var values []uint32
	for i := 0; i < 100; i++ {
		v := uint32(rand.Int31())
		values = append(values, v)
		bms.Add("test", v)
	}

	for _, v := range values {
		if !bms.Exists("test", v) {
			t.Errorf("expect %d exists but not found", v)
		}
	}

	for _, v := range values {
		bms.Remove("test", v)
	}

	for _, v := range values {
		if bms.Exists("test", v) {
			t.Errorf("expect %d non-exists but found it", v)
		}
	}

	for i := 0; i < 10; i++ {
		bms.AddMany("test", values[i*10:i*10+10])
	}
	for _, v := range values {
		if !bms.Exists("test", v) {
			t.Errorf("expect %d exists but not found", v)
		}
	}

	num := bms.Card("test")
	if num != 100 {
		t.Errorf("expect 100 elements but got %d", num)
	}
}

func TestBitmaps_Inter(t *testing.T) {
	bms := NewBitmaps()

	bms.AddMany("test1", []uint32{1, 2, 3, 10, 11})
	bms.AddMany("test2", []uint32{1, 2, 3, 20, 21})

	result := bms.Inter("test1", "test2")
	if result[0] != 1 || result[1] != 2 || result[2] != 3 {
		t.Fatalf("expect 1,2,3 but got %v", result)
	}
}

func TestBitmaps_Union(t *testing.T) {
	bms := NewBitmaps()

	bms.AddMany("test1", []uint32{1, 2, 3, 10, 11})
	bms.AddMany("test2", []uint32{1, 2, 3, 20, 21})

	result := bms.Union("test1", "test2")
	if len(result) != 7 || result[0] != 1 || result[1] != 2 || result[2] != 3 ||
		result[3] != 10 || result[4] != 11 ||
		result[5] != 20 || result[6] != 21 {
		t.Fatalf("expect 1,2,3,10,11,20,21 but got %v", result)
	}
}

func TestBitmaps_Xor(t *testing.T) {
	bms := NewBitmaps()

	bms.AddMany("test1", []uint32{1, 2, 3, 10, 11})
	bms.AddMany("test2", []uint32{1, 2, 3, 20, 21})

	result := bms.Xor("test1", "test2")
	if len(result) != 4 || result[0] != 10 || result[1] != 11 || result[2] != 20 || result[3] != 21 {
		t.Fatalf("expect 10,11,20,21 but got %v", result)
	}
}

func TestBitmaps_Diff(t *testing.T) {
	bms := NewBitmaps()

	bms.AddMany("test1", []uint32{1, 2, 3, 10, 11})
	bms.AddMany("test2", []uint32{1, 2, 3, 20, 21})

	result := bms.Diff("test1", "test2")
	if len(result) != 2 || result[0] != 10 || result[1] != 11 {
		t.Fatalf("expect 10,11 but got %v", result)
	}
}
