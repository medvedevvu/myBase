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

	table, err := myBase.GetTableByName(tableName)
	if err != nil {
		t.Errorf("таблица %s не найдена в базе  %s\n", tableName, err)
	}
	l_data := []byte(`test data`)
	err = table.Add(l_data)
	if err != nil {
		t.Errorf("добавление данных %v в таблицу %s не выполнено %s\n",
			l_data, tableName, err)
	}
	key := Key{utl.AsSha256(l_data), 0, 0, false}
	var value []byte
	value, err = table.GetRecByKey(key)
	if err != nil {
		t.Errorf("ошибка чтения %s \n", err)
	}
	if len(value) == 0 {
		t.Errorf("ничего не прочел %s \n", err)
	}

}
