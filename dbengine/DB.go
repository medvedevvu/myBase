package dbengine

import (
	"errors"
	"fmt"
	"path/filepath"
)

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
	tb := &Table{0, make(map[string]*Rec),
		&Index{"", make(map[string]*Key),
			make(map[int64]string)}, tableName}
	db.TblsList[obj] = tb                // добавили таблицу
	db.TblsList[obj].Storage = tableType // проставили тип хранилища
	db.IdxList[obj+"_idx"] = &Index{tableName + "_idx",
		make(map[string]*Key)}
	return nil
}

func (db *MyDB) Walk(execFunc FuncForWalk) error {
	for _, table := range db.TblsList { // все таблицы БД
		for key, rec := range table.Recs { // записи в таблице
			if table.Has([]byte(key)) { // удалена или нет
				err := execFunc([]byte(key), rec.Data)
				if err != nil {
					msg := fmt.Sprintf("ошибка исполнения ф-ции %s", err)
					return errors.New(msg)
				}
			}
		}
	}
	return nil
}

/*
после начала работы , через некоторое время записываем данные на диск
*/
func (db *MyDB) Digest() []error {
	log := []error{}
	for _, table := range db.TblsList { // обегаем все таблицы
		err := table.SaveItselfToDisk() //
		if err != nil {
			msg := fmt.Sprintf("индекс таблицы %s не сохранилася %s ", err)
			log = append(log, errors.New(msg))
		}
	}
	return log
}
