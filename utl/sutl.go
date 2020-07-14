package utl

import (
	"crypto/sha256"
	"errors"
	"fmt"
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
