package dbengine

import (
	"bytes"
	"errors"
	"fmt"
	utl "myBase/utl"
	"path"
	"strings"
)

type FuncForWalk func(key []byte, value []byte) error
type StorageTypeEnum uint8

const (
	memory StorageTypeEnum = iota
	onDisk
)

type Rec struct {
	Data []byte
}

type Table struct {
	Storage   StorageTypeEnum
	Recs      map[string]*Rec // string(key)
	TIndex    *Index
	TableName string
}

func (t *Table) Has(key []byte) bool {
	tk, ok := t.TIndex.Keys[string(key)]
	if !ok {
		return false
	}
	return !tk.IsDeleted // показываем только живых
}

func (t *Table) Add(key []byte, data []byte) error {
	if t.Has(key) {
		msg := fmt.Sprintf("ключ %v уже есть в таблице %s \n", key, t.TableName)
		return errors.New(msg)
	}
	t.Recs[string(key)] = &Rec{data} // записали в таблицу
	pos := int64(len(t.Recs))
	size := int64(len(data))
	ubyte := key
	ubyte = append(ubyte, data...)
	hash := utl.AsSha256(ubyte)
	vkey := &Key{hash, pos, size, false}
	t.TIndex.Keys[string(key)] = vkey // записали в индекс
	t.TIndex.KyesOrder[pos] = string(key)
	return nil
}

func (t *Table) GetValByKey(key []byte) ([]byte, error) {
	ok := t.Has(key)
	if !ok {
		return nil,
			errors.New(fmt.Sprintf("данные по ключу %v не найдены \n", key))
	}
	return t.Recs[string(key)].Data, nil
}

func (t *Table) GetIndexByKey(key []byte) (*Key, error) {
	ok := t.Has(key)
	if !ok {
		return nil,
			errors.New(fmt.Sprintf("данные по ключу %v не найдены \n", key))
	}
	return t.TIndex.Keys[string(key)], nil
}

func (t *Table) Delete(key []byte) bool {
	/* физически ничего не удаляем - затираем данные в индексе
	   isDelete = true
	*/
	ok := t.Has(key)
	if !ok {
		return false
	}
	indx, err := t.GetIndexByKey(key)
	if err != nil {
		return false
	}
	indx.IsDeleted = true
	return true
}

func (t *Table) Update(key []byte, newValue []byte) error {
	//добавим новый
	ok := t.Delete(key)
	if !ok {
		msg := fmt.Sprintf("ошибка обновления-удаление\n")
		return errors.New(msg)
	}
	err := t.Add(key, newValue)
	if err != nil {
		msg := fmt.Sprintf("ошибка %v обновления-добавление key=%v value=%v \n",
			err, key, newValue)
		return errors.New(msg)
	}
	return nil
}

func (t *Table) SaveItselfToDisk() error {
	// создадим файл индекса
	indxPath := t.TIndex.fileIndexName
	err := utl.CreateFile(indxPath)
	if err != nil {
		msg := fmt.Sprintf("ошибка %s создания файла индекса %s ",
			err, path.Base(indxPath))
		return errors.New(msg)
	}
	// создадим файл таблицы
	tablePath := strings.TrimSuffix(indxPath, "_idx") // получили таблицу
	if t.Storage == onDisk {
		err := utl.CreateFile(tablePath)
		if err != nil {
			msg := fmt.Sprintf("ошибка %s создания файла таблицы %s ",
				err, path.Base(tablePath))
			return errors.New(msg)
		}
	}
	// поробуем сохранить данные
	lenOfRecsInIndex := len(t.TIndex.Keys)
	if lenOfRecsInIndex == 0 {
		msg := fmt.Sprintf("индекс таблицы %s пустой", path.Base(tablePath))
		return errors.New(msg)
	}
	// в файл индекса пишем всегда !
	fileIndex, err := utl.GetFile(indxPath)
	defer fileIndex.Close()
	if err != nil {
		msg := fmt.Sprintf("не смогли открыть файл %s \n", err)
		return errors.New(msg)
	}
	for i := 0; i < len(t.TIndex.KyesOrder); i++ {
		ssKey := t.TIndex.KyesOrder[int64(i)]
		_, err := WriteDataToFileIndex(fileIndex, *t.TIndex.Keys[ssKey])
		if err != nil {
			msg := fmt.Sprintf("ошибка записи в файл %s \n", err)
			return errors.New(msg)
		}
	}
	// в файл таблицы только в этом случае !
	if t.Storage == onDisk {
		fileTable, err := utl.GetFile(tablePath)
		defer fileTable.Close()
		if err != nil {
			msg := fmt.Sprintf("не смогли открыть файл %s \n", err)
			return errors.New(msg)
		}
		for i := 0; i < len(t.TIndex.KyesOrder); i++ {
			ssKey := t.TIndex.KyesOrder[int64(i)]
			var bin_buf bytes.Buffer
			bin_buf.Write(t.Recs[ssKey].Data)
			n, err := fileTable.Write(bin_buf.Bytes())
			if err != nil || n == 0 {
				msg := fmt.Sprintf("не смогли записать %s  в файл %d байт \n", err, n)
				return errors.New(msg)
			}
		}
	}
	return nil
}
