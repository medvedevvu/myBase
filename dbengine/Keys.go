package dbengine

import (
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
	fileIndexName string
	queue         *Queue
}

func NewIndex(stType StorageTypeEnum, fileIndexName string) *Index {
	err := utl.CreateFile(fileIndexName + "_idx")
	if err != nil {
		panic("не могу создать файл индекса " +
			fileIndexName + "_idx")
	}
	return &Index{storageType: stType,
		fileIndexName: fileIndexName + "_idx",
		queue:         &Queue{}}
}

func (i *Index) AddKey(val interface{}, pos int64, size int64) {
	i.queue.Enqueue(&Key{hash: utl.AsSha256(val),
		pos: pos, size: size, isDeleted: false})
}

func (i *Index) Hash(hash string) bool {
	_, ok := i.queue.GetKeyByHash(hash, 0)
	return ok
}

func (i *Index) Delete(hash string) bool {
	return i.queue.Delete(hash)
}

func (i *Index) GetKeyByHash(hash string, what_kind int) (*Key, bool) {
	return i.queue.GetKeyByHash(hash, what_kind)
}

func (i *Index) Update(hash string, newValue Key) bool {
	return i.queue.Update(hash, newValue)
}

func (i *Index) PrintAll() {
	i.queue.PrintAll()
}
