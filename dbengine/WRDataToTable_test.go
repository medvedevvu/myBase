package dbengine

import (
	"errors"
	"fmt"
	utl "myBase/utl"
	"testing"
)

func CreateDbWithSingleTable(tableName string, vsstype StorageTypeEnum) (*MyDB, error) {
	sstype := vsstype
	myBase := NewMyDB(WorkingDir)
	utl.ClearDir(myBase.dbWorkDir, false)
	err := myBase.CreateTable(tableName, sstype)
	if err != nil {
		msg := fmt.Sprintf("Ошибка создания таблицы %v \n", err)
		return nil, errors.New(fmt.Sprintf(msg))
	}
	return myBase, nil
}

func AppendSomeDataToTableInBase(tableName string) (*Table, error) {
	myBase, err := CreateDbWithSingleTable(tableName, onDisk)
	if err != nil {
		msg := fmt.Sprintf("ошибка создания базы %v \n", err)
		return nil, errors.New(msg)
	}
	table, err := myBase.GetTableByName(tableName)
	if err != nil {
		msg := fmt.Sprintf("таблица %s не найдена в базе  %s\n", tableName, err)
		return nil, errors.New(msg)
	}
	for i := 0; i < 10; i++ {
		l_data := []byte(`test data`)
		l_data = append(l_data, []byte(fmt.Sprintf("%d", i))...)
		err = table.Add(l_data, l_data)
		//_ = table.AddDataToFile(l_data)
		if err != nil {
			msg := fmt.Sprintf("добавление данных %v в таблицу %s не выполнено %s\n",
				l_data, tableName, err)
			return nil, errors.New(msg)
		}
	}
	err = myBase.Digest()
	if err != nil {
		fmt.Printf("%s", err)
	}
	return table, nil
}

func TestWriteDataToTable(t *testing.T) {
	tableName := "table555"
	_, err := AppendSomeDataToTableInBase(tableName)
	if err != nil {
		t.Errorf("формирования тестовых данных %s \n", err)
	}

	/*	l_data := []byte(`test data7`)
		key := Key{utl.AsSha256(l_data), 0, 0, false, string(l_data)} // поисковый ключ
		err = table.Add(l_data, l_data)
		if err != nil {
			t.Errorf("ошибка добавления данных %s \n", err)
		}
		var value []byte
		value, err = table.GetRecByKey(key)
		if err != nil {
			t.Errorf("ошибка чтения %s \n", err)
		}
		if len(value) == 0 {
			t.Errorf("ничего не прочел %s \n", err)
		}
		if !reflect.DeepEqual(l_data, value) {
			t.Errorf("не верные данные got=%v <> wont=%v \n", l_data, value)
		}
	*/
}

func TestDeleteDataFromTable(t *testing.T) {
	tableName := "table78"
	table, err := AppendSomeDataToTableInBase(tableName)
	if err != nil {
		t.Errorf("формирования тестовых данных %s \n", err)
	}
	l_data := []byte(`test data7`)
	key := Key{utl.AsSha256(l_data), 0, 0, false, string(l_data)} // поисковый ключ
	ok, err := table.Delete([]byte(key.Kbyte))
	if !ok {
		t.Errorf("ошибка удаления данных по ключу %v ошибка %s \n", key, err)
	}
	// обновить файл индекса - БД метод Digest
}

func TestUpdateDataInTable(t *testing.T) {
	tableName := "table78"
	table, err := AppendSomeDataToTableInBase(tableName)
	if err != nil {
		t.Errorf("формирования тестовых данных %s \n", err)
	}
	oldData := []byte(`test data7`)
	key := Key{utl.AsSha256(oldData), 0, 0, false, string(oldData)} // поисковый ключ
	newData := []byte(`test data7777`)
	err = table.Update([]byte(key.Kbyte), newData)
	if err != nil {
		t.Errorf("ошибка обновлкения данных по ключу %v ошибка %s \n",
			key, err)
	}
	// обновить файл индекса - БД метод Digest
}
