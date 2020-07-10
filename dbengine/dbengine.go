package dbengine

import (
	"fmt"

	utl "myBase/utl"
)

// так как можем хранить все что угодно , в качестве типа записи
// в базе - пустой интерфейс

type MyDB struct {
	Tbls     map[string]interface{}
	TblsList []string
}

func NewMyDB() *MyDB {
	return &MyDB{Tbls: map[string]interface{}{}, TblsList: []string{}}
}

func (c *MyDB) AddTableList(tblList []string) error {
	if len(tblList) == 0 {
		return fmt.Errorf("Пустой список таблиц")
	}

	for _, tbl := range tblList {
		if !utl.Search(c.TblsList, tbl) {
			c.TblsList = append(c.TblsList, tbl)
		}
	}
	c.populateTables()
	return nil
}

func (c *MyDB) populateTables() {
	for _, tbs := range c.TblsList {
		c.Tbls[tbs] = make(map[string]interface{})
	}
}
