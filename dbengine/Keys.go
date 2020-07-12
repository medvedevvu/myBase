package dbengine

import (
	"fmt"
	utl "myBase/utl"
)

type Key struct {
	hash      string
	pos       int64
	size      int64
	isDeleted bool
}

/*
В дальнейшем полагаю , что таблицы храним в файлах 'name'
а индексы  в файлах 'name_idx'
*/

type Index struct {
	storageType   StorageTypeEnum
	key           []Key
	fileIndexName string
}

func NewIndex(stType StorageTypeEnum, fileIndexName string) *Index {
	err := utl.CreateFile(fileIndexName + "_idx")
	if err != nil {
		panic("не могу создать файл индекса " +
			fileIndexName + "_idx")
	}
	return &Index{storageType: stType,
		key: []Key{}, fileIndexName: fileIndexName + "_idx"}
}

func (i *Index) AddKey(val interface{}, pos int64, size int64) {
	i.key = append(i.key, Key{hash: utl.AsSha256(val),
		pos: pos, size: size, isDeleted: false})
}

func (i *Index) Hash(hash string) bool {
	for _, item := range i.key {
		if item.isDeleted {
			continue
		}
		if item.hash == hash {
			return true
		}
	}
	return false
}

func (i *Index) Delete(hash string) bool {
	for idx, item := range i.key {
		if item.isDeleted {
			continue
		}
		if item.hash == hash {
			item.isDeleted = true
			i.key[idx] = item
			return true
		}
	}
	return false
}

func (i *Index) GetKeyByHash(hash string, what_kind int) (Key, bool) {
	// what_kind = 0  только живых
	// what_kind = 1  всех
	for _, item := range i.key {
		if what_kind == 0 {
			if item.isDeleted {
				continue
			}
		}
		if item.hash == hash {
			return item, true
		}
	}
	return Key{}, false
}

func (i *Index) Update(hash string, newValue Key) bool {
	for idx, item := range i.key {
		if item.isDeleted {
			continue
		}
		if item.hash == hash {
			v := item
			v.pos = newValue.pos
			v.size = newValue.size
			i.key = append(i.key, v)
			item.isDeleted = true
			i.key[idx] = item
			return true
		}
	}
	return false
}

func (i *Index) PrintAll() {
	for idx, item := range i.key {
		fmt.Printf(" idx=%d value=%v \n", idx, item)
	}
}
