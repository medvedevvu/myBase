package dbengine

import (
	utl "myBase/utl"
	"testing"
)

func TestCreatDatabase(t *testing.T) {
	myDB := NewMyDB()
	tdlList := []string{"Table1"}
	err := myDB.AddTableList(tdlList)
	want := len(tdlList)
	got := len(myDB.TblsList)
	if want != got {
		t.Errorf("Таблица не добавилась want %d , got %d  !", want, got)
	}

	tdlList0 := []string{"Table1", "Table2", "Table3"}
	err = myDB.AddTableList(tdlList0)
	want = len([]string{"Table1", "Table2", "Table3"})
	got = len(tdlList0)
	if got != want {
		t.Errorf("Не добавился список таблиц want %d , got %d ", want, got)
	}

	tdlList1 := []string{}
	err = myDB.AddTableList(tdlList1)
	if err == nil {
		t.Errorf("Добавили пустой список таблиц")
	}

	tdlList2 := []string{"Table3", "Table3"}
	cnt := len(utl.RemoveRep(tdlList2))
	want = len(myDB.TblsList) + cnt
	_ = myDB.AddTableList(tdlList2)
	got = len(myDB.TblsList)
	if got != want {
		t.Errorf("Добавляем таблицы с одинаковыми именами want %d , got %d ", want, got)
	}

}
