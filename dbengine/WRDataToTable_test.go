package dbengine

import (
	"fmt"
	utl "myBase/utl"
	"testing"
)

func TestWriteDataToTable(t *testing.T) {
	sstype := onDisk
	myBase := NewMyDB(WorkingDir)
	utl.ClearDir(myBase.dbWorkDir, false)
	tableName := "table555"
	err := myBase.CreateTable(tableName, sstype)
	if err != nil {
		msg := fmt.Sprintf("Ошибка создания таблицы %v \n", err)
		t.Errorf(msg)
	}
	/*	value := []byte(`data`)
		key := Key{utl.AsSha256(value), 0, int64(len(value)), false}
		ok := myBase.Add(tableName, key, value)
		if !ok {
			t.Errorf("данные %v не добавились в таблицу %s ошибка %s \n",
				value, tableName, err)
		}
		ok = myBase.Has(key)
		if !ok {
			t.Errorf("ключ %v не найден \n", key)
		}*/
}
