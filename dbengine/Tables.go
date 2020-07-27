package dbengine

import (
	"errors"
	"fmt"
	utl "myBase/utl"
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
	Recs      map[string]*Rec
	TIndex    *Index
	TableName string
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
	return nil
}

func (t *Table) GetRecByKey(key []byte) ([]byte, error) {
	vk, ok := t.TIndex.Keys[string(key)]
	if !ok {
		return nil,
			errors.New(fmt.Sprintf("данные по ключу %v не найдены \n", key))
	}
	if vk.IsDeleted {
		return nil,
			errors.New(fmt.Sprintf("данные по ключу %v удалены \n", key))
	}
	return t.Recs[string(key)].Data, nil
}

func (t *Table) Delete(key []byte) (bool, error) {
	/* физически ничего не удаляем - затираем данные в индексе
	   isDelete = true
	*/
	ok := t.TIndex.Keys[string(key)].IsDeleted
	if !ok {
		msg := fmt.Sprintf("ключ %v не найден \n", key)
		return true, errors.New(msg)
	}
	t.TIndex.Keys[string(key)].IsDeleted = true
	return true, nil
}

func (t *Table) Update(key []byte, newValue []byte) error {
	//добавим новый
	ok, err := t.Delete(key)
	if !ok {
		msg := fmt.Sprintf("ошибка обновления %v - стадия удаления  \n", err)
		return errors.New(msg)
	}
	err = t.Add(key, newValue)
	if err != nil {
		msg := fmt.Sprintf("ошибка обновления %v - стадия добавления \n", err)
		return errors.New(msg)
	}
	return nil
}

func (t *Table) Has(key []byte) bool {
	tk, ok := t.TIndex.Keys[string(key)]
	if !ok {
		return false
	}
	return tk.IsDeleted
}
