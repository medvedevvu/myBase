package dbengine

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	utl "myBase/utl"
	"os"
	"reflect"
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
	//_, _ = file.Seek(0, 2)
	n, err := file.Write(bin_buf.Bytes())
	if err != nil || n == 0 {
		msg := fmt.Sprintf("не смогли записать %s  в файл %d байт \n", err, n)
		return 0, errors.New(msg)
	}
	return n, nil
}

func SearchInFileByKey(key Key, file *os.File) (bool, error) {
	i := 0
	vkey := -1
	var delta int64 = 4
	var bout_buf bytes.Buffer
	res := []byte{}
	tf := make([]byte, delta)
	_, err := file.Seek(0, 0)
	if err != nil {
		msg := fmt.Sprintf(" %s не смогли прочитать файл \n", err)
		return false, errors.New(msg)
	}
	for {
		n1, err := file.Read(tf)
		if err == io.EOF {
			//	достигнут конец файла
			break
		}
		if n1 == 0 || err != nil {
			msg := fmt.Sprintf("не смогли прочитать %s из файла %d байт \n", err, n1)
			return false, errors.New(msg)
		}
		i++ // стчётчик итераций
		if reflect.DeepEqual(tf, []byte(`\-\-`)) {
			// формируем прочитанные данные
			n1, err = bout_buf.Write(res)
			if err != nil || n1 == 0 {
				msg := fmt.Sprintf("не смогли прочитать %s в буфер %d байт \n", err, n1)
				return false, errors.New(msg)
			}
			dec := gob.NewDecoder(&bout_buf)
			var v Key
			err = dec.Decode(&v)
			if err != nil {
				msg := fmt.Sprintf("decode error %s :", err)
				return false, errors.New(msg)
			}
			//fmt.Printf("%v \n", v)
			res = nil // most importanat place !!!!!!!
			if reflect.DeepEqual(v, key) {
				vkey = i
				break
			}
			continue
		}
		cnt := utl.CountEmptyBytes(tf)
		if cnt > 0 {
			tf1 := utl.CleanEmptyByte(tf)
			res = append(res, tf1...)
			continue
		}
		res = append(res, tf...)
	} // end of loop
	if vkey < 0 {
		return false, nil
	}
	return true, nil
}
