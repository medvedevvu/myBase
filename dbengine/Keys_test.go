package dbengine

import (
	utl "myBase/utl"
	"testing"
)

func TestIndex(t *testing.T) {
	index := NewIndex(memory, "test")
	value := []byte(`test string`)
	index.AddKey(value, 0, 0)
	value = []byte(`test string1`)
	index.AddKey(value, 1, 1)
	want := utl.AsSha256(value)
	got := index.Hash(want)
	if !got {
		t.Errorf(" Добавленный Hash %v не найден got = %v \n", want, got)
	}

	value = []byte(`test string2`)
	want = utl.AsSha256(value)
	index.AddKey(value, 2, 2)
	index.Delete(want)
	got = index.Hash(want) // если true - значит не удалился !
	if got {
		t.Errorf(" Удаленный Hash %v не удалился got = %v \n", want, got)
	}

	value = []byte(`test string222`)
	newValue := []byte(`33333333333222`)
	newKeyValue := Key{hash: utl.AsSha256(newValue),
		pos: -111, size: 255, isDeleted: false}
	index.AddKey(value, 0, 0)
	got1, ok := index.GetKeyByHash(utl.AsSha256(value), 0)
	if !ok {
		t.Errorf("Не найдено значение ключа для %v \n", utl.AsSha256(value))
	}

	ok = index.Update(utl.AsSha256(value), newKeyValue)
	if !ok {
		t.Errorf("Не выполнена функция обновления для %v \n",
			newKeyValue)
	}
	want1, ok := index.GetKeyByHash(utl.AsSha256(value), 0)
	if !ok {
		t.Errorf("Не найдено значение ключа для %v после обновления \n", utl.AsSha256(value))
	}

	if !(got1.hash == want1.hash &&
		got1.pos != want1.pos &&
		got1.size != want1.size) {
		t.Errorf("Не выполнено обновление %v  %v \n", got1, want1)
	}

}
