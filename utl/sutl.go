package utl

import (
	"crypto/sha256"
	"fmt"
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
Все файлы создаем в текущей директории - где запущен файл
*/
func CreateFile(fname string) error {
	return nil
}
