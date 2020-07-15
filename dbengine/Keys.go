package dbengine

import (
	"errors"
	"fmt"
	utl "myBase/utl"
)

type Key struct {
	Hash      string
	Pos       int64
	Size      int64
	IsDeleted bool
}

/*
В дальнейшем полагаю , что таблицы храним в файлах 'name'
а индексы  в файлах 'name_idx'
*/

type Index struct {
	fileIndexName string // имя индекса , имя файла имя_idx
	queue         *Queue // очередь индекса
}

func NewIndex(fileIndexName string) (*Index, error) {
	err := utl.CreateFile(fileIndexName + "_idx")
	if err != nil {
		return nil, errors.New(fmt.Sprintf("не могу создать файл индекса "+
			fileIndexName+"_idx -- ошибка: %s", err))
	}
	return &Index{fileIndexName: fileIndexName + "_idx",
		queue: &Queue{}}, nil
}

func (i *Index) AddKey(val interface{}, pos int64, size int64) {
	/*	b, ok := val.([]byte)
		if !ok {
			return
		} */
	vkey := &Key{Hash: utl.AsSha256(val),
		Pos: pos, Size: size, IsDeleted: false}

	/*	_, err := utl.WriteToFile(i.fileIndexName, []byte(vkey))

		if !ok {
			return err
		} */

	i.queue.Enqueue(vkey)
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
