package dbengine

import (
	"errors"
	"fmt"
	"math"
	"math/rand"
	utl "myBase/utl"
	"path/filepath"
	"testing"
	"time"
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
			adds := rand.Intn(recIntable)
			l_data = append(l_data, []byte(fmt.Sprintf("%d", i*adds))...)
			err = table.Add(l_data, l_data)
			if err == nil {
				_ = table.AddDataToFile(l_data)
				pos := len(table.Recs)
				lkey := Key{utl.AsSha256(l_data), int64(pos),
					int64(len(l_data)), false, string(l_data)}
				_ = table.TIndex.AddDataToFile(lkey)
			}
		}
	}
	return myBase, nil
}

func TestRestore(t *testing.T) {
	recsIntable := 11 // записей в таблицу
	cntTable := 3     // кол-во таблиц
	_, err := DBforTests(cntTable, recsIntable, true)
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
	// посмотрим лог ошибок
	for _, l := range logs {
		msg := fmt.Sprintf("%s \n", l)
		t.Errorf(msg)
	}
}

func TestWalk(t *testing.T) {
	// создадим тестовый ландшафт
	recsIntable := 1 // записей в таблицу
	cntTable := 3    // кол-во таблиц
	_, err := DBforTests(cntTable, recsIntable, true)
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
	// попробуем пройти по базе
	// type FuncForWalk func(key, value []byte) error
	// попробуем пройти по базе
	var f FuncForWalk
	f = func(key []byte, value []byte) error {
		s := string(value)
		fmt.Printf("ключ %v значение %s\n", key, s)
		return nil
	}
	err = myBase.Walk(f)
	if err != nil {
		msg := fmt.Sprintf("проход по базе окончился с ошибкой %s \n", err)
		t.Errorf(msg)
	}
	var f1 FuncForWalk
	f1 = func(key []byte, value []byte) error {
		s := string(value)
		msg := fmt.Sprintf("ключ %v значение %s\n", key, s)
		return errors.New(msg)
	}
	err = myBase.Walk(f1)
	if err == nil {
		msg := fmt.Sprintf("ожидаем ошибку %s \n", err)
		t.Errorf(msg)
	}
}

func TestDigest(t *testing.T) {
	// создадим тестовый ландшафт
	recsIntable := 2 // записей в таблицу
	cntTable := 3    // кол-во таблиц
	db, err := DBforTests(cntTable, recsIntable, false)
	if err != nil {
		msg := fmt.Sprintf("ошибка создания тестовых данных %s \n", err)
		t.Errorf(msg)
	}
	// тестируем метод обновления данных на диске
	// теперь добавим в базу
	tb, err := db.GetTableByName("Table1")
	if err != nil {
		msg := fmt.Sprintf("ошибка получения таблицы данных %s \n", err)
		t.Errorf(msg)
	}
	for i := 0; i < 3; i++ {
		vPos := i * 100
		vSize := i
		adds := fmt.Sprintf("%d%d", vPos, vSize)
		value := []byte("test")
		value = append(value, adds...)
		err := tb.Add(value, value)
		if err != nil {
			t.Errorf("ошибка %s добавления в таблицу данных %s \n", err, value)
		}
	}
	time.Sleep(time.Millisecond * 2)
	err = db.Digest()
	if err != nil {
		msg := fmt.Sprintf("ошибка обновления данных %s \n", err)
		t.Errorf(msg)
	}
	// поробуем сделать recjvery но на другой БД
	AnotherDB := NewMyDB(WorkingDir)
	AnotherDB.Restore() // пробуем восстановиться
	_, err = db.GetTableByName("Table1")
	if err != nil {
		msg := fmt.Sprintf("ошибка обновления данных %s \n", err)
		t.Errorf(msg)
	}
}

func TestStopBase(t *testing.T) {
	// создадим тестовый ландшафт
	recsIntable := 2 // записей в таблицу
	cntTable := 3    // кол-во таблиц
	db, err := DBforTests(cntTable, recsIntable, false)
	if err != nil {
		msg := fmt.Sprintf("ошибка создания тестовых данных %s \n", err)
		t.Errorf(msg)
	}
	// тестируем метод обновления данных на диске
	// теперь добавим в базу
	tb, err := db.GetTableByName("Table1")
	if err != nil {
		msg := fmt.Sprintf("ошибка получения таблицы данных %s \n", err)
		t.Errorf(msg)
	}
	for i := 0; i < 3; i++ {
		vPos := i * 100
		vSize := i
		adds := fmt.Sprintf("%d%d", vPos, vSize)
		value := []byte("test")
		value = append(value, adds...)
		err := tb.Add(value, value)
		if err != nil {
			t.Errorf("ошибка %s добавления в таблицу данных %s \n", err, value)
		}
	}
	time.Sleep(time.Millisecond * 2)
	errs := db.Stop()
	if err != nil {
		for _, e := range errs {
			t.Errorf("%s \n", e)
		}
	}
}
