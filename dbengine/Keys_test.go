package dbengine

import (
	utl "myBase/utl"
	"testing"
)

func TestIndexInMemory(t *testing.T) {
	index := NewIndex(memory, "test")
	value := []byte(`test string`)
	index.AddKey(value, 0, 0)
	want := utl.AsSha256(value)
	got := index.Hash(want)
	if !got {
		t.Errorf(" Добавленный Hash %v не найден got = %v \n", want, got)
	}

	value = []byte(`test string1`)
	index.AddKey(value, 0, 0)
	want = utl.AsSha256(value)
	index.Delete(want)
	got = index.Hash(want)
	if !got {
		t.Errorf(" Удаленный Hash %v не удалился got = %v \n", want, got)
	}

}
