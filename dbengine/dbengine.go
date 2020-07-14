package dbengine

import (
	"fmt"

	utl "myBase/utl"
)

// так как можем хранить все что угодно , в качестве типа записи
// в базе - пустой интерфейс

type StorageTypeEnum uint8

type MyDB struct {
	Tbls      map[string]interface{}
	TblsList  []string
	dbWorkDir string
}

const (
	memory StorageTypeEnum = iota
	onDisk
)

func NewMyDB(dbwrkdir string) *MyDB {
	return &MyDB{Tbls: make(map[string]interface{}),
		TblsList: []string{}, dbWorkDir: dbwrkdir}
}

func (c *MyDB) CreateTable(tblList []string, ttype StorageTypeEnum) error {
	if len(tblList) == 0 {
		return fmt.Errorf("Пустой список таблиц")
	}

	for _, tbl := range tblList {
		if !utl.Search(c.TblsList, tbl) {
			c.TblsList = append(c.TblsList, tbl)
		}
	}
	val := c.populateTables(ttype)
	if len(val) > 0 {
		return fmt.Errorf("Пропущено создание таблиц %v \n", val)
	}
	return nil
}

func (c *MyDB) populateTables(ttype StorageTypeEnum) map[string]error {
	errMap := map[string]error{}
	for _, tbs := range c.TblsList {
		err := c.saveTables(ttype)
		if err != nil {
			errMap[tbs] = err
			continue
		}
		c.Tbls[tbs] = make(map[string]interface{})
	}
	return errMap
}

// здесь будем создавать таблицу с пустым ключом в памяти или ф/с
func (c *MyDB) saveTables(ttype StorageTypeEnum) error {
	if ttype == memory {
		return nil
	}
	if ttype == onDisk {
		return nil
	}
	return nil
}

func (c MyDB) GetTblCount() int {
	return len(c.Tbls)
}

func (c *MyDB) GetTblByName(name string) (error, interface{}) {
	val, ok := c.Tbls[name]
	if !ok {
		return fmt.Errorf(" таблица не найдена %s", name), nil
	}
	return nil, val
}
