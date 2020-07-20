package dbengine

import (
	"fmt"
	utl "myBase/utl"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

var FlSep string = string(filepath.Separator)
var WorkingDir string = `c:\out` // потом возьмем у БД

func TestCreateIndexFile(t *testing.T) {
	var got string = WorkingDir + FlSep + "test"
	var want string = WorkingDir + FlSep + "test_idx"
	utl.ClearFiles(got)
	utl.ClearFiles(want)
	var err error
	_, err = NewIndex(got)
	if err != nil {
		t.Errorf("ошибка процедуры создания индекса %s \n", err)
	}
	if _, err = os.Stat(want); os.IsNotExist(err) {
		t.Errorf("файл индекса %s не создался в каталоге %s \n", want, WorkingDir)
	}
}

func TestAppendSmallDataToIndexFile(t *testing.T) {
	got := WorkingDir + FlSep + "test1"
	// если файл есть - удалить
	utl.ClearFiles(got + "_idx")
	var err error
	Indx, err := NewIndex(got)
	if err != nil {
		t.Errorf(" не создан индекс %s", err)
	}
	l_hash := utl.AsSha256([]byte(`t333333333`))
	l_size := int64(0)
	l_pos := int64(9)
	wantKey := Key{Hash: l_hash, Pos: l_pos, Size: l_size, IsDeleted: false}
	err = Indx.Add(wantKey)

	if err != nil {
		t.Errorf("не прошло добавление ключа %v в индекс %s \n", wantKey, err)
	}

	ok := Indx.Hash(wantKey)
	if !ok {
		msg := fmt.Sprintf(" ключь %v не найден \n", wantKey)
		t.Errorf(msg)
	}
	gotKey, ok := Indx.GetKeyByHash(wantKey, 0)

	if reflect.DeepEqual(wantKey, gotKey) {
		msg := fmt.Sprintf(" want= %v не равен got=%v \n", wantKey, gotKey)
		t.Errorf(msg)
	}
	if Indx.GetLen() != 1 {
		t.Errorf(" Key %v не добавился длина без изменений \n", gotKey)
	}
}

func TestUpdateIndexFile(t *testing.T) {
	got := WorkingDir + FlSep + "test52"
	// если файл есть - удалить
	utl.ClearFiles(got + "_idx")
	var err error
	Indx, err := NewIndex(got)
	if err != nil {
		t.Errorf(" не создан индекс %s", err)
	}
	for i := 0; i < 5; i++ {
		vPos := i * 100
		vSize := i
		adds := fmt.Sprintf("%d%d", vPos, vSize)
		value := []byte(`test`)
		value = append(value, adds...)
		wantKey := Key{utl.AsSha256(value), int64(vPos), int64(vSize), false}
		err = Indx.Add(wantKey)
		if err != nil {
			t.Errorf("не прошло добавление ключа %v в индекс %s \n", wantKey, err)
		}
	}
	vPos := 400
	vSize := 4
	adds := fmt.Sprintf("%d%d", vPos, vSize)
	value := []byte(`test`)
	value = append(value, adds...)
	keyExist := Key{utl.AsSha256(value), int64(vPos), int64(vSize), false}

	ok := Indx.Hash(keyExist)
	if !ok {
		msg := fmt.Sprintf("ключ %v нет в базе !!! %v \n", keyExist, ok)
		t.Errorf(msg)
	}

	keyNotExist := Key{utl.AsSha256(`12121221`), 333, 3, false}

	ok = Indx.Hash(keyNotExist)
	if ok {
		msg := fmt.Sprintf("ключ %v уже есть в базе !!! \n", keyNotExist)
		t.Errorf(msg)
	}
	ok = Indx.Update(keyExist, keyNotExist)

	if !ok {
		msg := fmt.Sprintf("обновление %v на %v не прошло \n", keyExist, keyNotExist)
		t.Errorf(msg)
	}

	ok = Indx.Hash(keyExist)
	if ok {
		msg := fmt.Sprintf("существующий ключ %v не удален \n", keyExist)
		t.Errorf(msg)
	}

	ok = Indx.Hash(keyNotExist)
	if !ok {
		msg := fmt.Sprintf("новый ключ %v не добавлен \n", keyNotExist)
		t.Errorf(msg)
	}

}

func TestDeleteDataIndexFile(t *testing.T) {
	got := WorkingDir + FlSep + "test52"
	// если файл есть - удалить
	utl.ClearFiles(got + "_idx")
	var err error
	Indx, err := NewIndex(got)
	if err != nil {
		t.Errorf(" не создан индекс %s", err)
	}
	willRemove := Key{}
	for i := 0; i < 4; i++ {
		vPos := i * 100
		vSize := i
		adds := fmt.Sprintf("%d%d", vPos, vSize)
		value := []byte(`test`)
		value = append(value, adds...)
		wantKey := Key{utl.AsSha256(value), int64(vPos), int64(vSize), false}
		if i == 2 {
			willRemove = wantKey
		}
		err = Indx.Add(wantKey)
		if err != nil {
			t.Errorf("не прошло добавление ключа %v в индекс %s \n", wantKey, err)
		}
	}

	ok := Indx.Hash(willRemove)
	if !ok {
		msg := fmt.Sprintf(" ключ %v не найден %v \n", willRemove, ok)
		t.Errorf(msg)
	}

	ok = Indx.Delete(willRemove)
	if !ok {
		msg := fmt.Sprintf(" не выполнено удаление ключа %v  %v \n", willRemove, ok)
		t.Errorf(msg)
	}

	ok = Indx.Hash(willRemove)
	if ok {
		msg := fmt.Sprintf("удаленный ключ %v найден в базе %v \n", willRemove, ok)
		t.Errorf(msg)
	}
}
