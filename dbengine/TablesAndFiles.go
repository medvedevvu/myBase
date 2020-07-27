package dbengine

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"strings"
)

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

func (t *Table) AddDataToFile(data []byte) error {
	obj := t.TIndex.fileIndexName         // отпилю _idx с головы
	obj = strings.TrimSuffix(obj, "_idx") // получили таблицу
	file, err := os.OpenFile(obj, os.O_APPEND|os.O_CREATE|os.O_RDWR, os.ModePerm)
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
