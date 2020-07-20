package dbengine

import (
	"fmt"
	"myBase/utl"
	"testing"
)

func TestCreateTable(t *testing.T) {
	sstype := onDisk
	myBase := NewMyDB(WorkingDir)
	utl.ClearDir(myBase.dbWorkDir, false)
	tableName := "table1"
	err := myBase.CreateTable(tableName, sstype)
	if err != nil {
		msg := fmt.Sprintf("Ошибка создания таблицы %v \n", err)
		t.Errorf(msg)
	}

	_, err = myBase.GetTableByName(tableName)
	if err != nil {
		msg := fmt.Sprintf("Tаблицы %s нет в базе \n", tableName)
		t.Errorf(msg)
	}
	// создадим одноименную таблицу ещё раз
	err = myBase.CreateTable(tableName, sstype)
	if err == nil {
		msg := fmt.Sprintf("Tаблицa %s уже есть в базе  %s \n", tableName, err)
		t.Errorf(msg)
	}
	tbl, err := myBase.GetTableFileByName(tableName)
	if err != nil {
		msg := fmt.Sprintf("Ошибка поиска таблицы %s -- %s \n",
			tbl, err)
		t.Errorf(msg)
	}

	_, ok := myBase.IdxList[tbl+"_idx"]
	if !ok {
		if tbl != "memory" {
			msg := fmt.Sprintf("Индекса %s для таблицы %s нет в базе \n",
				tbl+"_idx", tableName)
			t.Errorf(msg)
		}
	}
	// так как добавили всего одну таблицу , то и индекс должен быть 1
	got := len(myBase.IdxList)
	want := 1
	if got != want {
		msg := fmt.Sprintf("Индексов должно быть %d а получили %d \n",
			want, got)
		t.Errorf(msg)
	}

}
