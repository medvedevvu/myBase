package dbengine

import (
	"errors"
	"fmt"
	"myBase/utl"
	"os"
	"path/filepath"
	"reflect"
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
}

type MyDB struct {
	dbWorkDir string            // рабочий каталог базы
	TblsList  map[string]*Table // Таблицы
	IdxList   map[string][]*Key // Ключи
}

func NewMyDB(dbwrkdir string) *MyDB {
	lworkdir := dbwrkdir + string(filepath.Separator)
	return &MyDB{TblsList: make(map[string]*Table),
		IdxList: make(map[string][]*Key), dbWorkDir: lworkdir}
}

func (db *MyDB) CreateTable(tableName string, tableType StorageTypeEnum) error {
	obj := db.dbWorkDir + tableName
	_, ok := db.TblsList[obj]
	if ok {
		msg := fmt.Sprintf("Tаблица %s уже есть в базе \n", tableName)
		return errors.New(msg)
	}
	db.TblsList[obj] = &Table{}          // добавили таблицу
	db.TblsList[obj].Storage = tableType // проставили тип хранилища
	if tableType == onDisk {             // на диске создаем
		err := utl.CreateFile(obj)
		if err != nil {
			msg := fmt.Sprintf("ошибка создания файла %s \n", err)
			return errors.New(msg)
		}
	}
	db.IdxList[obj+"_idx"] = append(db.IdxList[obj+"_idx"], &Key{}) // добавили индекс
	err := utl.CreateFile(obj + "_idx")
	if err != nil {
		msg := fmt.Sprintf("ошибка создания файла %s \n", err)
		return errors.New(msg)
	}
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
		if reflect.DeepEqual(key, idxKey) {
			return true
		}
	}
	return false
}

func (db *MyDB) Add(tableName string, key Key, value []byte) bool {
	obj := db.dbWorkDir + tableName
	v_pos := len(db.TblsList[obj].Recs)
	db.TblsList[obj].Recs[key].Data = value
	db.TblsList[obj].Recs[key].Pos = int64(v_pos)
	db.TblsList[obj].Recs[key].Size = int64(len(value))

	// IdxList   map[string]*Key
	//(db.IdxList[obj+"_idx"]).Add( key)

	if db.TblsList[obj].Storage == onDisk {
		// пишем в файл таблицы
	}
	return true
}
