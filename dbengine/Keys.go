package dbengine

import (
	rnd "math/rand"
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
	for _, item := range i.key {
		if item.isDeleted {
			continue
		}
		if item.hash == hash {
			item.isDeleted = true
			return true
		}
	}
	return false
}

func (i *Index) Update(hash string) bool {
	for _, item := range i.key {
		if item.isDeleted {
			continue
		}
		if item.hash == hash {
			v := item
			v.pos = rnd.Int63n(999)  // времнные заглушки
			v.size = rnd.Int63n(999) // времнные заглушки
			item.isDeleted = true
			i.key = append(i.key, v)
			return true
		}
	}
	return false
}
