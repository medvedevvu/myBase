package dbengine

import (
	utl "myBase/utl"
	"testing"
)

func TestIndex(t *testing.T) {
	index, _ := NewIndex(WorkingDir + FlSep + "test")
	value := []byte(`test string`)
	key1 := Key{utl.AsSha256(value), 0, 0, false}
	index.Add(key1)
	value = []byte(`test string1`)
	key2 := Key{utl.AsSha256(value), 1, 1, false}
	index.Add(key2)
	//want := key2.Hash
	got := index.Hash(key2)
	if !got {
		t.Errorf(" Добавленный Key %v не найден got = %v \n", key2, got)
	}

	value = []byte(`test string2`)
	//want := utl.AsSha256(value)
	key1 = Key{utl.AsSha256(value), 2, 2, false}
	index.Add(key1)
	index.Delete(key1)
	got = index.Hash(key1) // если true - значит не удалился !
	if got {
		t.Errorf(" Удаленный Key %v не удалился got = %v \n", key1, got)
	}

	value = []byte(`test string222`)
	key1 = Key{utl.AsSha256(value), 0, 0, false}

	newValue := []byte(`33333333333222`)
	key2 = Key{Hash: utl.AsSha256(newValue),
		Pos: -111, Size: 255, IsDeleted: false}

	index.Add(key1)

	got1, ok := index.GetKeyByHash(key1, 0)
	if !ok {
		t.Errorf("Не найден добавленый Key %v \n", got1)
	}

	ok = index.Update(key1, key2)
	if !ok {
		t.Errorf("Не выполнена функция обновления для %v \n", key2)
	}
	_, ok = index.GetKeyByHash(key1, 0)
	if ok {
		t.Errorf("Найден Key %v после обновления \n", key1)
	}

}
