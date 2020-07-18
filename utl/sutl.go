package utl

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
	"os"
)

// Поиск элемента
func Search(tmp []string, str string) bool {
	for idx := range tmp {
		if str == tmp[idx] {
			return true
		}
	}
	return false
}

// Убрать повторы элементов
func RemoveRep(tmp []string) []string {
	res := []string{}
	for _, val := range tmp {
		if !Search(tmp, val) {
			res = append(res, val)
		}
	}
	return res
}

// получить Hash от любого объекта
func AsSha256(o interface{}) string {
	h := sha256.New()
	h.Write([]byte(fmt.Sprintf("%v", o)))
	return fmt.Sprintf("%x", h.Sum(nil))
}

/*
Если файл существует , пробуем его открыть
*/
func CreateFile(fname string) error {
	f, err := os.OpenFile(fname, os.O_CREATE, 0644)
	if err != nil {
		return errors.New(fmt.Sprintf(" ошибка создания открытия %s", err))
	}
	if err != nil {
		err = f.Close()
		return errors.New(fmt.Sprintf(" ошибка закрытия %s", err))
	}
	return nil
}

func WriteToFile(fname string, val []byte) (int64, error) {
	f, err := os.OpenFile(fname, os.O_APPEND|os.O_CREATE, 0664)
	if err != nil {
		return 0, err
	}
	n, err := f.Write(val)
	if err != nil {
		return 0, err
	}
	return int64(n), err
}

func ClearFiles(pathFile string) error {
	if _, err := os.Stat(pathFile); err == nil {
		err = os.Remove(pathFile)
		if err != nil {
			v := pathFile
			msg := fmt.Sprintf("ошибка %s удаления файла %s \n", err, v)
			return errors.New(msg)
		}
	}
	return nil
}

func Set4ByteRange(bin_buf *bytes.Buffer) error {
	// буду выравнивать данные по сегментам в 4 байта а в конец
	d := int(math.Mod(float64(bin_buf.Len()), 4))
	if d > 0 {
		n, err := bin_buf.Write(make([]byte, 4-d))
		if err != nil {
			msg := fmt.Sprintf("не выполнено выравнивание %s в %d байт \n", err, n)
			return errors.New(msg)
		}
	}
	// записывать []byte{`\0\0`} - маркер окончания данных
	n, err := bin_buf.Write([]byte(`\0\0`))
	if err != nil {
		msg := fmt.Sprintf("не дописан маркер %s в %d байт \n", err, n)
		return errors.New(msg)
	}
	return nil
}

func CountEmptyBytes(s []byte) int {
	var tmp byte
	var count int = 0
	for _, a := range s {
		if a == tmp {
			count += 1
		}
	}
	return count
}

func CleanEmptyByte(s []byte) []byte {
	var tmp byte
	arr := []byte{}
	for i := 0; i < len(s); i++ {
		if s[i] != tmp {
			arr = append(arr, s[i])
		}
	}
	return arr
}
