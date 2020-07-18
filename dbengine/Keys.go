package dbengine

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	utl "myBase/utl"
	"os"
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

func (i Index) GetLen() int {
	return i.queue.Length
}

func (i Index) GetKey(index int) (*Key, error) {
	que_size := i.GetLen()
	if que_size < index {
		return nil, errors.New(
			fmt.Sprintf("позиция %d больше длины %d \n", index, que_size))
	}
	file, err := os.Open(i.fileIndexName)
	defer file.Close()
	if err != nil {
		msg := fmt.Sprintf("Ошибка %s чтения индексного файла \n", err)
		return nil, errors.New(msg)
	}

	n := 0
	var bout_buf bytes.Buffer
	tf := []byte{}
	n, err = file.Read(tf)
	if err != nil || n == 0 {
		msg := fmt.Sprintf("не смогли прочитать %s из файла %d байт \n", err, n)
		return nil, errors.New(msg)
	}
	n, err = bout_buf.Write(tf)
	if err != nil || n == 0 {
		msg := fmt.Sprintf(" не смогли прочитать %s в буфер %d байт \n", err, n)
		return nil, errors.New(msg)
	}
	dec := gob.NewDecoder(&bout_buf)
	var v Key
	err = dec.Decode(&v)
	if err != nil {
		return nil, errors.New(
			fmt.Sprintf("decoder error %s  \n", err))
	}
	return &v, nil
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

func (i *Index) Add(key Key) error {
	file, err := os.OpenFile(i.fileIndexName, os.O_APPEND|os.O_CREATE, 0664)
	defer file.Close()
	if err != nil {
		msg := fmt.Sprintf("файл %s не читается %s  \n", i.fileIndexName, err)
		return errors.New(msg)
	}
	var bin_buf bytes.Buffer
	enc := gob.NewEncoder(&bin_buf)
	err = enc.Encode(key)
	if err != nil {
		msg := fmt.Sprintf("encode error: %s", err)
		return errors.New(msg)
	}
	n, err := file.Write(bin_buf.Bytes())
	if err != nil || n == 0 {
		msg := fmt.Sprintf("не смогли записать %s  в файл %d байт \n", err, n)
		return errors.New(msg)
	}
	i.queue.Enqueue(&key)
	return nil
}

func (i *Index) Hash(key Key) bool {
	_, ok := i.queue.GetKeyByHash(key.Hash, 0)
	return ok
}

func (i *Index) Delete(key Key) bool {
	return i.queue.Delete(key.Hash)
}

func (i *Index) GetKeyByHash(key Key, what_kind int) (*Key, bool) {
	return i.queue.GetKeyByHash(key.Hash, what_kind)
}

func (i *Index) Update(key Key, newKey Key) bool {
	return i.queue.Update(key.Hash, newKey)
}

func (i *Index) PrintAll() {
	i.queue.PrintAll()
}

func WriteDataToFile(file *os.File, temp Key) (int, error) {
	var bin_buf bytes.Buffer
	enc := gob.NewEncoder(&bin_buf)
	err := enc.Encode(&temp)
	if err != nil {
		msg := fmt.Sprintf("encode error: %s", err)
		return 0, errors.New(msg)
	}
	err = utl.Set4ByteRange(&bin_buf)
	if err != nil {
		msg := fmt.Sprintf(" ошибка вырвнивания %s \n", err)
		return 0, errors.New(msg)
	}
	_, _ = file.Seek(0, 2)
	n, err := file.Write(bin_buf.Bytes())
	if err != nil || n == 0 {
		msg := fmt.Sprintf("не смогли записать %s  в файл %d байт \n", err, n)
		return 0, errors.New(msg)
	}
	return n, nil
}
