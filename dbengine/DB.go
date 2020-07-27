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
		&Index{"", make(map[string]*Key)}, tableName}
	db.TblsList[obj] = tb                // добавили таблицу
	db.TblsList[obj].Storage = tableType // проставили тип хранилища
	db.IdxList[obj+"_idx"] = &Index{tableName + "_idx",
		make(map[string]*Key)}
	return nil
}

/*
func (db *MyDB) Walk(execFunc FuncForWalk) error {
	for fname, idx := range db.IdxList {
		this := idx.queue
		v_tmp := this.Peek()
		for {
			if v_tmp != nil {
				// получить список строк таблицы
				recs := db.TblsList[strings.TrimSuffix(fname, "_idx")].Recs
				key := Key{v_tmp.Value.Hash,
					v_tmp.Value.Pos,
					v_tmp.Value.Size,
					v_tmp.Value.IsDeleted, v_tmp.Value.Kbyte}
				err := execFunc([]byte(key.Kbyte), recs[key].Data)
				if err != nil {
					msg := fmt.Sprintf("ошибка %s исполнения ф-ции "+
						"с ключом %v и данными %v ", err, key, recs[key].Data)
					return errors.New(msg)
				}
				v_tmp = v_tmp.Next
				continue
			}
			break
		}
	}
	return nil
}
*/
