package dbengine

import (
	"os"
	"path/filepath"
	"testing"
)

var FlSep string = string(filepath.Separator)
var WorkingDir string = `c:\out` // потом возьмем у БД

func TestCreateIndexFile(t *testing.T) {
	got := WorkingDir + FlSep + "test"
	want := WorkingDir + FlSep + "test_idx"
	var err error
	_, err = NewIndex(got)
	if err != nil {
		t.Errorf("ошибка процедуры создания индекса %s \n", err)
	}
	if _, err = os.Stat(want); os.IsNotExist(err) {
		t.Errorf("файл индекса %s не создался в каталоге %s \n", want, WorkingDir)
	}
}

func TestAppendDataToIndexFile(t *testing.T) {
	got := WorkingDir + FlSep + "test"
	var err error
	Indx, err := NewIndex(got)
	if err != nil {
		t.Errorf(" не создан индекс %s", err)
	}
	value := []byte(`test data`)

	// как в файл таблицы
	f, err := os.OpenFile(got, os.O_APPEND|os.O_CREATE, 0664)
	if err != nil {
		t.Errorf(" не открыт файл на добавление %s", err)
	}
	n, err := f.Write(value)
	//
	Indx.AddKey(value, int64(n), 0)

}
