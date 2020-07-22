package dbengine

import (
	"bytes"
	"errors"
	"fmt"
	"myBase/utl"
	"os"
	"path/filepath"
	"reflect"
	"strings"
)

// так как можем хранить все что угодно , в качестве типа записи
// в базе - пустой интерфейс

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
	var out []byte
	rkey, ok := t.TIndex.GetKeyByHash(key, 0)
	if !ok {
		return nil,
			errors.New(fmt.Sprintf("данные по ключу %v не найдены \n", key))
	}
	if t.Storage == onDisk {
		file, err := os.Open(strings.TrimSuffix(t.TIndex.fileIndexName, "_idx"))
		defer file.Close()
		if err != nil {
			return nil,
				errors.New(fmt.Sprintf("ошибка %s поиска на диске \n", err))
		}
		// определим позицию
		cs := t.TIndex.queue.CountSeek(rkey.Pos)
		file.Seek(cs, 0)
		out = make([]byte, rkey.Size)
		n, err := file.Read(out)
		if err != nil {
			return nil,
				errors.New(
					fmt.Sprintf("ошибка %s чтения даты из файла \n", err))
		}
		if n == 0 {
			return nil,
				errors.New(
					fmt.Sprintf("прочитано даты из файла %d байт \n", n))
		}
	}
	if t.Storage == onDisk {
		if !reflect.DeepEqual(out, t.Recs[*rkey].Data) {
			return nil,
				errors.New(
					fmt.Sprintf("данные в памяти не совпадают с данными в файле \n"))
		}
	}
	return t.Recs[*rkey].Data, nil
}
