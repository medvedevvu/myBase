package dbengine

import (
	"errors"
	"fmt"
	"math"
	utl "myBase/utl"
	"path/filepath"
	"testing"
)

type Tf struct {
	TableName string
	TableType StorageTypeEnum
}

func DBforTests(tblCount int, recIntable int, loaddata bool) (*MyDB, error) {
	tblList := make([]Tf, 0)
	for i := 0; i < tblCount; i++ {
		tblName := fmt.Sprintf("Table%d", i)
		r := math.Mod(float64(i), 3)
		var tblType StorageTypeEnum
		if int64(r) == 0 {
			tblType = memory
		} else {
			tblType = onDisk
		}
		tblList = append(tblList, Tf{tblName, tblType})
	}
	myBase := NewMyDB(WorkingDir)
	// создамдим таблицы и индексы в базе
	utl.ClearDir(myBase.dbWorkDir, false)
	for _, itTbl := range tblList {
		err := myBase.CreateTable(itTbl.TableName, itTbl.TableType)
		if err != nil {
			msg := fmt.Sprintf("Ошибка создания таблицы %v \n", err)
			return nil, errors.New(fmt.Sprintf(msg))
		}
	}
	if !loaddata { // не заполнять данными
		return myBase, nil
	}

	for _, itTbl := range tblList {
		table, err := myBase.GetTableByName(itTbl.TableName)
		if err != nil {
			msg := fmt.Sprintf("таблица %s не найдена в базе  %s\n",
				itTbl.TableName, err)
			return nil, errors.New(msg)
		}
		for i := 0; i < recIntable; i++ {
			l_data := []byte(`test_data`)
			l_data = append(l_data, []byte(fmt.Sprintf("%d", i))...)
			err = table.Add(l_data)
			if err != nil {
				msg := fmt.Sprintf("добавление данных %v в таблицу %s не выполнено %s\n",
					l_data, itTbl.TableName, err)
				return nil, errors.New(msg)
			}
		}
	}
	return myBase, nil
}

func TestRestore(t *testing.T) {
	_, err := DBforTests(10, 10, true)
	if err != nil {
		msg := fmt.Sprintf("ошибка создания тестовых данных %s \n", err)
		t.Errorf(msg)
	}
	myBase := NewMyDB(WorkingDir)
	ok, err, logs := myBase.Restore()
	if !ok {
		msg := fmt.Sprintf("ошибка восстановления %s %v\n", err, logs)
		t.Errorf(msg)
	}
	if len(myBase.IdxList) == 0 {
		msg := fmt.Sprintf("индексы не восстановились \n")
		t.Errorf(msg)
	}
	if len(myBase.TblsList) == 0 {
		msg := fmt.Sprintf("таблицы не восстановились \n")
		t.Errorf(msg)
	}
	if len(myBase.TblsList) != len(myBase.IdxList) {
		msg := fmt.Sprintf("разное кол-во таблиц и индексов \n")
		t.Errorf(msg)
	}

	for tblName, tbl := range myBase.TblsList {
		if len(tbl.Recs) == 0 {
			msg := fmt.Sprintf("нет данных в структуре таблицы %s \n",
				filepath.Base(tblName))
			t.Errorf(msg)
		}
		if tbl.TIndex.queue.Len() == 0 {
			msg := fmt.Sprintf("нет данных в структуре индекса %s \n",
				filepath.Base(tblName)+"_idx")
			t.Errorf(msg)
		}
	}
}
