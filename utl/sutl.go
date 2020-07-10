package utl

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
