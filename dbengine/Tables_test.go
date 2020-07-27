package dbengine

import (
	"errors"
	"fmt"
	"reflect"
	"testing"
)

var WorkingDir string = `c:\out` // потом возьмем у БД

func TempDb() *MyDB {
	db := NewMyDB(WorkingDir)
	return db
}
func TestCreateTabel(t *testing.T) {
	db := TempDb()
	err := db.CreateTable("table1", memory)
	if err != nil {
		msg := fmt.Sprintf("ошибка создания таблицы %s \n", err)
		t.Fatal(errors.New(msg))
	}
	_, err = db.GetTableByName("table1")
	if err != nil {
		msg := fmt.Sprintf("ошибка чтения таблицы %s \n", err)
		t.Fatal(errors.New(msg))
	}
	_, err = db.GetTableByName("table2")
	if err == nil {
		msg := fmt.Sprintf("таблицы %s нет в базе \n", "table2")
		t.Fatal(errors.New(msg))
	}
}

func TestAdd(t *testing.T) {
	db := TempDb()
	tableName := "table1"
	err := db.CreateTable(tableName, memory)
	if err != nil {
		msg := fmt.Sprintf("ошибка создания таблицы %s \n", err)
		t.Fatal(errors.New(msg))
	}
	tbl, err := db.GetTableByName(tableName)
	if err != nil {
		msg := fmt.Sprintf("таблицы %s нет в базе \n", "table2")
		t.Fatal(errors.New(msg))
	}
	tKey := []byte(`key1`)
	tValue := []byte(`value value`)
	err = tbl.Add(tKey, tValue)
	if err != nil {
		msg := fmt.Sprintf("ошибка добавления key=%v value=%v \n", tKey, tValue)
		t.Fatal(errors.New(msg))
	}
	vbyte, err := tbl.GetRecByKey(tKey)
	if err != nil {
		msg := fmt.Sprintf("ошибка %s получения значения по key=%v \n", err, tKey)
		t.Fatal(errors.New(msg))
	}
	if !reflect.DeepEqual(tValue, vbyte) {
		msg := fmt.Sprintf(" want=%v  got=%v \n", tValue, vbyte)
		t.Fatal(errors.New(msg))
	}
}
