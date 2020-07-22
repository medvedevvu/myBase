package dbengine

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"myBase/utl"
	"os"
	"path/filepath"
	"reflect"
	"strings"
)

type StorageTypeEnum uint8

const (
	memory StorageTypeEnum = iota
	onDisk
)

type Rec struct {
	Pos  int64
	Size int64
	Data []byte
}

type Table struct {
	Storage StorageTypeEnum
	Recs    map[Key]*Rec
	TIndex  *Index
}

type MyDB struct {
	dbWorkDir string            // рабочий каталог базы
	TblsList  map[string]*Table // Таблицы
	IdxList   map[string]*Index // Ключи
}

func NewMyDB(dbwrkdir string) *MyDB {
	lworkdir := dbwrkdir + string(filepath.Separator)
	return &MyDB{TblsList: make(map[string]*Table),
		IdxList: make(map[string]*Index), dbWorkDir: lworkdir}
}

func (db *MyDB) CreateTable(tableName string, tableType StorageTypeEnum) error {
	obj := db.dbWorkDir + tableName
	_, ok := db.TblsList[obj]
	if ok {
		msg := fmt.Sprintf("Tаблица %s уже есть в базе \n", tableName)
		return errors.New(msg)
	}
	db.TblsList[obj] = &Table{0, make(map[Key]*Rec), &Index{}} // добавили таблицу
	db.TblsList[obj].Storage = tableType                       // проставили тип хранилища
	if tableType == onDisk {                                   // на диске создаем
		err := utl.CreateFile(obj)
		if err != nil {
			msg := fmt.Sprintf("ошибка создания файла %s \n", err)
			return errors.New(msg)
		}
	}
	midx, err := NewIndex(obj)    // создали индекс
	db.IdxList[obj+"_idx"] = midx // добавили индекс
	//	err = utl.CreateFile(obj + "_idx")  // NewIndex сам создаст файл
	if err != nil {
		msg := fmt.Sprintf("ошибка создания файла %s \n", err)
		return errors.New(msg)
	}
	db.TblsList[obj].TIndex = midx // прилепили индекс
	return nil
}
func (db *MyDB) Restore() (bool, error, []string) {
	objList := []string{}
	objList, err := utl.OSReadDir(db.dbWorkDir)
	if err != nil {
		msg := fmt.Sprintf("ошибка получения данных о структуре %s\n", err)
		return false, errors.New(msg), nil
	}
	if len(objList) == 0 {
		msg := fmt.Sprintf("не найдены объекты \n")
		return false, errors.New(msg), nil
	}
	// Определимся с местом хранения таблиц
	// если есть _idx файл , но нет table - значит таблица
	// хранилась в памяти - восстановлению не подлежит
	// - данные потеряны  безвозвратно - УНИЧТОЖИТЬ!!!!!
	tbls := []string{}   // таблицы
	indxs := []string{}  // индексы
	recovs := []string{} // будут восстановлены
	for _, fl := range objList {
		if strings.HasSuffix(fl, "_idx") {
			indxs = append(indxs, fl)
		} else {
			tbls = append(tbls, fl)
		}
	}
mainloop:
	for _, fl := range indxs {
		for _, tb := range tbls {
			if strings.TrimSuffix(fl, "_idx") == tb {
				recovs = append(recovs, tb)
				continue mainloop
			}
		}
	}
	// попробуем загрузить
	log_restore := []string{}
	for _, rec := range recovs {
		err := db.CreateTable(rec, onDisk)
		if err != nil {
			msg := fmt.Sprintf("создание таблицы %s - ошибка %s", rec, err)
			log_restore = append(log_restore, msg)
			continue
		}
		t, err := db.GetTableByName(rec)
		err = t.LoadData()
		if err != nil {
			msg := fmt.Sprintf("загрузка таблицы %s - ошибка %s", rec, err)
			log_restore = append(log_restore, msg)
			continue
		}
	}
	return true, nil, log_restore
}

func (t *Table) LoadData() error {
	// загрузим дату из проиницированных
	file, err := os.Open(t.TIndex.fileIndexName)
	if err != nil {
		msg := fmt.Sprintf(" ошибка восстановления индекса \n")
		return errors.New(msg)
	}
	// ---------------------
	i := 0
	_, err = file.Seek(0, 0)
	if err != nil {
		msg := fmt.Sprintf(" %s не смогли прочитать файл \n", err)
		return errors.New(msg)
	}
	res := []byte{}
	tf := make([]byte, 1)
	for {
		n1, err := file.Read(tf)
		if err == io.EOF {
			break
		}
		if n1 == 0 || err != nil {
			msg := fmt.Sprintf("не смогли прочитать %s из файла %d байт \n", err, n1)
			return errors.New(msg)
		}
		i++ // стчётчик итераций
		if reflect.DeepEqual(tf, []byte(`|`)) {
			// формируем прочитанные данные
			var bout_buf bytes.Buffer
			n1, err = bout_buf.Write(res)
			if err != nil || n1 == 0 {
				msg := fmt.Sprintf("не смогли прочитать %s в буфер %d байт \n", err, n1)
				return errors.New(msg)
			}
			dec := gob.NewDecoder(&bout_buf)
			var v Key
			err = dec.Decode(&v)
			if err != nil {
				msg := fmt.Sprintf("decode error %s :", err)
				return errors.New(msg)
			}
			// вставляем
			t.TIndex.queue.Enqueue(&v)
			res = nil // most importanat place !!!!!!!
			continue
		}
		res = append(res, tf...)
	}
	// ---------------------
	return nil
}

func (db *MyDB) GetTableByName(tableName string) (*Table, error) {
	obj := db.dbWorkDir + tableName
	tbl, ok := db.TblsList[obj]
	if !ok {
		return nil, errors.New(fmt.Sprintf("Таблица %s не найдена \n", tableName))
	}
	return tbl, nil
}

func (db *MyDB) GetIndexByTableName(tableName string) (*Index, error) {
	obj := db.dbWorkDir + tableName + "_idx"
	idx, ok := db.IdxList[obj]
	if !ok {
		return nil, errors.New(fmt.Sprintf("Индекс %s не найдена \n", tableName+"_idx"))
	}
	return idx, nil
}

func (db *MyDB) GetTableFileByName(tableName string) (string, error) {
	vt, err := db.GetTableByName(tableName)
	if err != nil {
		return "", errors.New(fmt.Sprintf("ошибка поиска таблицы %s  \n ", tableName))
	}
	if vt.Storage == memory { // таблица создавалась в памяти
		return "memory", nil
	}
	obj := db.dbWorkDir + tableName
	if _, err := os.Stat(obj); err == nil {
		return obj, nil
	}
	return "", errors.New(fmt.Sprintf("файл таблица %s не найден \n ", tableName))
}

func (db *MyDB) GetTableIndexFileByName(tableName string) (string, error) {
	obj := db.dbWorkDir + tableName + "_idx"
	if _, err := os.Stat(obj); err == nil {
		return obj, nil
	}
	return "", errors.New(fmt.Sprintf("файл индекса %s_idx не найден \n ", tableName))
}

func (db *MyDB) Has(key Key) bool {
	for _, idxKey := range db.IdxList {
		if idxKey.Has(key) {
			return true
		}
	}
	return false
}

func (t *Table) AddDataToFile(data []byte) error {
	obj := t.TIndex.fileIndexName         // отпилю _idx с головы
	obj = strings.TrimSuffix(obj, "_idx") // получили таблицу
	file, err := os.OpenFile(obj, os.O_APPEND|os.O_CREATE, 0664)
	defer file.Close()
	if err != nil {
		msg := fmt.Sprintf("файл %s не читается %s  \n", obj, err)
		return errors.New(msg)
	}
	var bin_buf bytes.Buffer
	bin_buf.Write(data)
	n, err := file.Write(bin_buf.Bytes())
	if err != nil || n == 0 {
		msg := fmt.Sprintf("не смогли записать %s  в файл %d байт \n", err, n)
		return errors.New(msg)
	}
	return nil
}

func (t *Table) Add(data []byte) error {
	pos := len(t.Recs)
	key := Key{utl.AsSha256(data), int64(pos), int64(len(data)), false}
	rec := &Rec{Pos: int64(pos), Size: int64(len(data)), Data: data}
	t.Recs[key] = rec
	if t.Storage == onDisk {
		err := t.AddDataToFile(data)
		if err != nil {
			return errors.New(fmt.Sprintf("ошибка записи в файл таблицы %s \n", err))
		}
	}
	// теперь надо записать в индекс
	err := t.TIndex.Add(key)
	if err != nil {
		return errors.New(fmt.Sprintf(" ошибка индекса %s \n", err))
	}
	return nil
}

func (t *Table) GetRecByKey(key Key) ([]byte, error) {
	rkey, ok := t.TIndex.GetKeyByHash(key, 0)
	if !ok {
		return nil,
			errors.New(fmt.Sprintf("данные по ключу %v не найдены \n", key))
	}
	return t.Recs[*rkey].Data, nil
}

func (t *Table) Delete(key Key) (bool, error) {
	/* физически ничего не удаляем - затираем данные в индексе
	   isDelete = true
	*/
	ok := t.TIndex.queue.Delete(key.Hash)
	if !ok {
		msg := "ошибка удаления из кучи"
		return true, errors.New(msg)
	}
	ok = t.TIndex.Delete(key)
	if !ok {
		msg := "ошибка удаления из файла"
		return true, errors.New(msg)
	}
	return true, nil
}

func (t *Table) Update(key Key, newValue []byte) error {
	//добавим новый
	err := t.Add(newValue)
	if err != nil {
		msg := fmt.Sprintf("ошибка обновления %v - стадия добавления \n", err)
		return errors.New(msg)
	}
	ok, err := t.Delete(key)
	if !ok {
		msg := fmt.Sprintf("ошибка обновления %v - стадия удаления  \n", err)
		return errors.New(msg)
	}
	return nil
}
