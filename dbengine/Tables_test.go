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

func TempDbAndSmallTable(tableName string) (*MyDB, error) {
	db := NewMyDB(WorkingDir)
	err := db.CreateTable(tableName, memory)
	if err != nil {
		msg := fmt.Sprintf("ошибка создания таблицы %s \n", err)
		return nil, errors.New(msg)
	}
	return db, nil
}

func TestCreateTabel(t *testing.T) {
	db := TempDb()
	err := db.CreateTable("table1", memory)
	if err != nil {
		msg := fmt.Sprintf("ошибка создания таблицы %s \n", err)
		t.Error(errors.New(msg))
	}
	_, err = db.GetTableByName("table1")
	if err != nil {
		msg := fmt.Sprintf("ошибка чтения таблицы %s \n", err)
		t.Error(errors.New(msg))
	}
	_, err = db.GetTableByName("table2")
	if err == nil {
		msg := fmt.Sprintf("таблицы %s нет в базе \n", "table2")
		t.Error(errors.New(msg))
	}
}

func TestAdd(t *testing.T) {
	tableName := "table1"
	db, err := TempDbAndSmallTable(tableName)
	if err != nil {
		msg := fmt.Sprintf("ошибка создания БД с таблицей %s \n", tableName)
		t.Error(errors.New(msg))
	}
	tbl, err := db.GetTableByName(tableName)
	if err != nil {
		msg := fmt.Sprintf("таблицы %s нет в базе \n", tableName)
		t.Error(errors.New(msg))
	}
	tKey := []byte(`key1`)
	tValue := []byte(`value value`)
	err = tbl.Add(tKey, tValue)
	if err != nil {
		msg := fmt.Sprintf("ошибка добавления key=%v value=%v \n", tKey, tValue)
		t.Error(errors.New(msg))
	}
	vbyte, err := tbl.GetValByKey(tKey)
	if err != nil {
		msg := fmt.Sprintf("ошибка %s получения значения по key=%v \n", err, tKey)
		t.Error(errors.New(msg))
	}
	if !reflect.DeepEqual(tValue, vbyte) {
		msg := fmt.Sprintf(" want=%v  got=%v \n", tValue, vbyte)
		t.Error(errors.New(msg))
	}
}

func TestUpdate(t *testing.T) {
	newValue := []byte(`111`) // для обновления
	tableName := "table1"
	db, err := TempDbAndSmallTable(tableName)
	if err != nil {
		msg := fmt.Sprintf("ошибка создания БД с таблицей %s \n", tableName)
		t.Error(errors.New(msg))
	}
	tbl, err := db.GetTableByName(tableName)
	if err != nil {
		msg := fmt.Sprintf("таблицы %s нет в базе \n", "table2")
		t.Error(errors.New(msg))
	}
	for i := 0; i < 10; i++ {
		tKey := []byte(`key`)
		tKey = append(tKey, []byte(fmt.Sprintf("%d", i))...)
		tValue := []byte(`valuevalue`)
		tValue = append(tValue, []byte(fmt.Sprintf("%d", i))...)
		err = tbl.Add(tKey, tValue)
		if err != nil {
			msg := fmt.Sprintf("ошибка добавления key=%v value=%v \n", tKey, tValue)
			t.Error(errors.New(msg))
		}
	}
	for i := 0; i < 10; i++ {
		tKey := []byte(`key`)
		tKey = append(tKey, []byte(fmt.Sprintf("%d", i))...)
		val, err := tbl.GetValByKey(tKey)
		if err != nil {
			msg := fmt.Sprintf("ошибка поиска по ключу key=%v value=%v \n",
				tKey, val)
			t.Error(errors.New(msg))
		}
		err = tbl.Update(tKey, newValue)
		if err != nil {
			msg := fmt.Sprintf("ошибка обновления по ключу key=%v value=%v \n",
				tKey, newValue)
			t.Error(errors.New(msg))
		}
	}
	for i := 0; i < 10; i++ {
		tKey := []byte(`key`)
		tKey = append(tKey, []byte(fmt.Sprintf("%d", i))...)
		val, err := tbl.GetValByKey(tKey)
		if err != nil {
			msg := fmt.Sprintf("ошибка поиска по ключу key=%v value=%v \n",
				tKey, val)
			t.Error(errors.New(msg))
		}
		if !reflect.DeepEqual(val, newValue) {
			msg := fmt.Sprintf("не выполнено обновление"+
				"по ключу key=%v value=%v \n", tKey, val)
			t.Error(errors.New(msg))
		}
	}
}

func TestDelete(t *testing.T) {
	tableName := "table1"
	db, err := TempDbAndSmallTable(tableName)
	if err != nil {
		msg := fmt.Sprintf("ошибка создания БД с таблицей %s \n", tableName)
		t.Error(errors.New(msg))
	}
	tbl, err := db.GetTableByName(tableName)
	if err != nil {
		msg := fmt.Sprintf("таблицы %s нет в базе \n", "table2")
		t.Error(errors.New(msg))
	}
	for i := 0; i < 10; i++ {
		tKey := []byte(`key`)
		tKey = append(tKey, []byte(fmt.Sprintf("%d", i))...)
		tValue := []byte(`valuevalue`)
		tValue = append(tValue, []byte(fmt.Sprintf("%d", i))...)
		err = tbl.Add(tKey, tValue)
		if err != nil {
			msg := fmt.Sprintf("ошибка добавления key=%v value=%v \n", tKey, tValue)
			t.Error(errors.New(msg))
		}
	}
	for i := 9; i < 0; i-- {
		tKey := []byte(`key`)
		tKey = append(tKey, []byte(fmt.Sprintf("%d", i))...)
		ok := tbl.Delete(tKey)
		if !ok {
			msg := fmt.Sprintf("ошибка удаления key=%v \n", tKey)
			t.Error(errors.New(msg))
		}
	}
	for i := 9; i < 0; i-- {
		tKey := []byte(`key`)
		tKey = append(tKey, []byte(fmt.Sprintf("%d", i))...)
		ok := tbl.Has(tKey)
		if !ok {
			msg := fmt.Sprintf("найден удаленный ключ key=%v \n", tKey)
			t.Error(errors.New(msg))
		}
	}
}
