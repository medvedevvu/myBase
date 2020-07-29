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
	Hash string
	//TKey      []byte // ключ из таблицы
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
	Keys          map[string]*Key
	KyesOrder     map[int64]string // порядок ключей
}

func WriteDataToFileIndex(file *os.File, temp Key) (int, error) {
	var bin_buf bytes.Buffer
	enc := gob.NewEncoder(&bin_buf)
	err := enc.Encode(&temp)
	if err != nil {
		msg := fmt.Sprintf("encode error: %s", err)
		return 0, errors.New(msg)
	}
	err = utl.AppStopByte(&bin_buf)
	if err != nil {
		msg := fmt.Sprintf(" ошибка вырвнивания %s \n", err)
		return 0, errors.New(msg)
	}
	n, err := file.Write(bin_buf.Bytes())
	if err != nil || n == 0 {
		msg := fmt.Sprintf("не смогли записать %s  в файл %d байт \n", err, n)
		return 0, errors.New(msg)
	}
	return n, nil
}
