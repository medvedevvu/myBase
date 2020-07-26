package dbengine

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	utl "myBase/utl"
	"sync"
	"testing"
	"time"
)

func DBforTSTests(tblCount int, recIntable int, loaddata bool) (*MyDB, error) {
	tblList := make([]Tf, 0)
	for i := 0; i < tblCount; i++ {
		tblName := fmt.Sprintf("Table%d", i)
		tblList = append(tblList, Tf{tblName, onDisk})
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
	myBase.Digest()
	return myBase, nil
}

/*
func TestAddUpdateDeletewithSingleTable(t *testing.T) {
	// создадим тестовый ландшафт
	tbCount := 1    // кол-во таблиц при создании
	recIntab := 100 // кол-во строк при создании

	db, err := DBforTSTests(tbCount, recIntab, true)
	if err != nil {
		msg := fmt.Sprintf("ошибка создания базы %s \n", err)
		t.Errorf(msg)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	res := make(chan []string, tbCount)
	wg.Add(3)

	go func(ctx context.Context, tableName string, db *MyDB) {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				fmt.Printf("Выход %s из обработки update \n", tableName)
				return
			case res <- UpdateDataTable(tableName, db):
			}
		}
	}(ctx, fmt.Sprintf("Table%d", 0), db)

	go func(ctx context.Context, tableName string, db *MyDB) {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				fmt.Printf("Выход %s из обработки insert \n", tableName)
				return
			case res <- insertDataToTable(db, tableName, recIntab):
			}
		}
	}(ctx, fmt.Sprintf("Table%d", 0), db)

	go func(ctx context.Context, tableName string, db *MyDB) {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				fmt.Printf("Выход %s из обработки delete \n", tableName)
				return
			case res <- DeleteDataTable(tableName, db):
			}
		}
	}(ctx, fmt.Sprintf("Table%d", 0), db)

	/*go func(ctx context.Context, db *MyDB) {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				fmt.Printf("Выход из обработки digest \n")
				return
			case res <- DigestTbl(db):
			}
		}
	}(ctx, db)
*/
/*	time.Sleep(1 * time.Second) // подождем
	cancel()
	wg.Wait()
	close(res)
	for v := range res {
		for _, it := range v {
			fmt.Printf("%s\n", it)
		}
	}
	//_ = db.Digest()
} */

func DigestTbl(db *MyDB) []string {
	log := []string{}
	for i := 0; i < 10; i++ {
		time.Sleep(10 * time.Millisecond)
		err := db.Digest()
		if err != nil {
			log = append(log, fmt.Sprintf("%s", err))
		}
		v := fmt.Sprintf("дайджест окончен итерация %d", i)
		log = append(log, v)
	}
	return log
}
func TestDeleteSameTableOpts(t *testing.T) {
	// создадим тестовый ландшафт
	tbCount := 3   // кол-во таблиц при создании
	recIntab := 10 // кол-во строк при создании

	db, err := DBforTSTests(tbCount, recIntab, true)
	if err != nil {
		msg := fmt.Sprintf("ошибка создания базы %s \n", err)
		t.Errorf(msg)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	res := make(chan []string, tbCount)
	wg.Add(tbCount)

	for i := 0; i < tbCount; i++ {
		go func(ctx context.Context, tableName string, db *MyDB) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					fmt.Printf("Выход %s из обработки \n", tableName)
					return
				case res <- DeleteDataTable(tableName, db):
				}
			}
		}(ctx, fmt.Sprintf("Table%d", 1), db)
	}
	time.Sleep(1 * time.Second) // подождем
	cancel()
	wg.Wait()
	close(res)
	for v := range res {
		for _, it := range v {
			fmt.Printf("%s\n", it)
		}
	}
}
func TestDeleteBaseOpts(t *testing.T) {
	// создадим тестовый ландшафт
	tbCount := 3   // кол-во таблиц при создании
	recIntab := 10 // кол-во строк при создании

	db, err := DBforTSTests(tbCount, recIntab, true)
	if err != nil {
		msg := fmt.Sprintf("ошибка создания базы %s \n", err)
		t.Errorf(msg)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	res := make(chan []string, tbCount)
	wg.Add(tbCount)

	for i := 0; i < tbCount; i++ {
		go func(ctx context.Context, tableName string, db *MyDB) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					fmt.Printf("Выход %s из обработки \n", tableName)
					return
				case res <- DeleteDataTable(tableName, db):
				}
			}
		}(ctx, fmt.Sprintf("Table%d", i), db)
	}
	time.Sleep(1 * time.Second) // подождем
	cancel()
	wg.Wait()
	close(res)
	for v := range res {
		for _, it := range v {
			fmt.Printf("%s\n", it)
		}
	}
}
func DeleteDataTable(tableName string, db *MyDB) []string {
	log := []string{}
	tb, err := db.GetTableByName(tableName)
	if err != nil {
		msg := fmt.Sprintf("%s при открытия %s ", err, tableName)
		log = append(log, msg)
	}
	// пробегаю по всем ключам индекса таблицы
	v_tmp := tb.TIndex.queue.Peek()
	for {
		if v_tmp != nil {
			key := []byte(tb.TIndex.queue.Start.Value.Kbyte)
			ok, err := tb.Delete(key)
			if !ok {
				msg := fmt.Sprintf("%s при удалении %s", err, tableName)
				log = append(log, msg)
			}
			v_tmp = v_tmp.Next
			continue
		}
		break
	}
	log = append(log, fmt.Sprintf("обработка таблицы %s окончена", tableName))
	return log
}

func TestUpdateSameTableOpts(t *testing.T) {
	// создадим тестовый ландшафт
	tbCount := 4  // кол-во таблиц при создании
	recIntab := 3 // кол-во строк при создании

	db, err := DBforTSTests(tbCount, recIntab, true)
	if err != nil {
		msg := fmt.Sprintf(" ошибка создания базы %s \n", err)
		t.Errorf(msg)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	res := make(chan []string, tbCount)
	wg.Add(tbCount)

	for i := 0; i < tbCount; i++ {
		go func(ctx context.Context, tableName string, db *MyDB) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					fmt.Printf("Выход %s из обработки \n", tableName)
					return
				case res <- UpdateDataTable(tableName, db):
				}
			} // обновляем 1 таблицу
		}(ctx, fmt.Sprintf("Table%d", 1), db)
	}
	time.Sleep(1 * time.Second) // подождем
	cancel()
	wg.Wait()
	close(res)
	for v := range res {
		for _, it := range v {
			fmt.Printf("%s\n", it)
		}
	}
}

func TestUpdateBaseOpts(t *testing.T) {
	// создадим тестовый ландшафт
	tbCount := 3   // кол-во таблиц при создании
	recIntab := 10 // кол-во строк при создании

	db, err := DBforTSTests(tbCount, recIntab, true)
	if err != nil {
		msg := fmt.Sprintf("ошибка создания базы %s \n", err)
		t.Errorf(msg)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	res := make(chan []string, tbCount)
	wg.Add(tbCount)

	for i := 0; i < tbCount; i++ {
		go func(ctx context.Context, tableName string, db *MyDB) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					fmt.Printf("Выход %s из обработки \n", tableName)
					return
				case res <- UpdateDataTable(tableName, db):
				}
			}
		}(ctx, fmt.Sprintf("Table%d", i), db)
	}
	time.Sleep(1 * time.Second) // подождем
	cancel()
	wg.Wait()
	close(res)
	for v := range res {
		for _, it := range v {
			fmt.Printf("%s\n", it)
		}
	}
}
func UpdateDataTable(tableName string, db *MyDB) []string {
	log := []string{}
	tb, err := db.GetTableByName(tableName)
	if err != nil {
		msg := fmt.Sprintf("%s при открытия %s ", err, tableName)
		log = append(log, msg)
	}
	// пробегаю по всем ключам индекса таблицы
	v_tmp := tb.TIndex.queue.Peek()
	for {
		if v_tmp != nil {
			key := []byte(tb.TIndex.queue.Start.Value.Kbyte)
			newValue := []byte(`111111`)
			err = tb.Update(key, newValue)
			if err != nil {
				msg := fmt.Sprintf("%s при обновлении %s ", err, tableName)
				log = append(log, msg)
			}
			v_tmp = v_tmp.Next
			continue
		}
		break
	}
	log = append(log, fmt.Sprintf("обработка таблицы %s окончена", tableName))
	return log
}

func TestAddBaseOpts(t *testing.T) {
	// создадим тестовый ландшафт
	tbCount := 1              // кол-во таблиц при создании
	recIntab := 10            // кол-во строк при создании
	recIntableInserting := 10 // кол-во строк при работе в горутинах

	db, err := DBforTSTests(tbCount, recIntab, false)
	if err != nil {
		msg := fmt.Sprintf(" ошибка создания базы %s \n", err)
		t.Errorf(msg)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	res := make(chan []string, tbCount)

	wg.Add(tbCount)

	for i := 0; i < tbCount; i++ {
		go func(ctx context.Context, tableName string,
			db *MyDB, recIntable int) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					fmt.Printf("Выход %s из обработки \n", tableName)
					return
				case res <- insertDataToTable(db, tableName, recIntable):
				}
			}
		}(ctx, fmt.Sprintf("Table%d", i), db, recIntableInserting)
	}
	time.Sleep(1 * time.Second) // подождем
	cancel()
	wg.Wait()
	close(res)
	for v := range res {
		for _, it := range v {
			fmt.Printf("%s\n", it)
		}
	}
}

func insertDataToTable(db *MyDB, tableName string, recIntable int) []string {
	log := []string{}
	tb, err := db.GetTableByName(tableName)
	if err != nil {
		msg := fmt.Sprintf("%s при открытия %s ", err, tableName)
		log = append(log, msg)
	}
	for i := 0; i < recIntable; i++ {
		l_data := []byte(`test_data`)
		adds := rand.Intn(recIntable)
		l_data = append(l_data, []byte(fmt.Sprintf("%d", i*adds))...)
		err = tb.Add(l_data, l_data)
		if err != nil {
			msg := fmt.Sprintf("%s загрузки %s ", err, tableName)
			log = append(log, msg)
		}
	}
	log = append(log, fmt.Sprintf("загрузка в %s окончена", tableName))
	return log
}

func TestAddToSameTable(t *testing.T) {
	// создадим тестовый ландшафт
	tbCount := 4              // кол-во таблиц при создании
	recIntab := 3             // кол-во строк при создании
	recIntableInserting := 10 // кол-во строк при работе в горутинах

	db, err := DBforTSTests(tbCount, recIntab, false)
	if err != nil {
		msg := fmt.Sprintf(" ошибка создания базы %s \n", err)
		t.Errorf(msg)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	res := make(chan []string, tbCount)
	wg.Add(tbCount)
	for i := 0; i < tbCount; i++ {
		go func(ctx context.Context, tableName string,
			db *MyDB, recIntable int) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					fmt.Printf("Выход %s из обработки \n", tableName)
					return
				case res <- insertDataToTable(db, tableName, recIntable):
					return
				}
			} // вставляем всё в  1 таблицу
		}(ctx, fmt.Sprintf("Table%d", 1), db, recIntableInserting)
	}
	time.Sleep(1 * time.Second) // подождем
	cancel()
	wg.Wait()
	close(res)
	for v := range res {
		for _, it := range v {
			fmt.Printf("%s\n", it)
		}
	}
}
