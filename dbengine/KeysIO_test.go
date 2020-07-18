package dbengine

import (
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
	if _, err := os.Stat(got + "_idx"); err == nil {
		err = os.Remove(got + "_idx")
		if err != nil {
			v := got + "_idx"
			t.Errorf("ошибка %s удаления файла %s \n", err, v)
		}
	}
	var err error
	Indx, err := NewIndex(got)
	if err != nil {
		t.Errorf(" не создан индекс %s", err)
	}

	l_hash := utl.AsSha256([]byte(`t333333333`))
	l_size := int64(0) // int64(len([]byte(`test3`)))
	l_pos := int64(9)
	wantKey := Key{Hash: l_hash, Pos: l_pos, Size: l_size, IsDeleted: false}
	Indx.Add(wantKey)
	gotKey, ok := Indx.GetKeyByHash(wantKey, 0)

	if !ok ||
		wantKey.Hash != gotKey.Hash ||
		wantKey.Pos != gotKey.Pos ||
		wantKey.Size != gotKey.Size ||
		wantKey.IsDeleted != gotKey.IsDeleted {
		t.Errorf(" want= %v не равен got=%v \n", wantKey, gotKey)
	}

	if Indx.GetLen() != 1 {
		t.Errorf(" Key %v не добавился длина без изменений \n", gotKey)
	}
	key1, err := Indx.GetKey(0)
	if err != nil {
		t.Errorf(" Key %v не прочитан из файла %s \n", key1, err)
	}
	if !reflect.DeepEqual(*key1, wantKey) {
		t.Errorf("got=%v \n  want=%v \n", key1, wantKey)
	}
}
