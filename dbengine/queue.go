package dbengine

import "fmt"

type (
	Queue struct {
		Start, End *Node
		Length     int
	}
	Node struct {
		Value *Key
		Next  *Node
	}
)

// Конструктор
func New() *Queue {
	return &Queue{nil, nil, 0}
}

// Снять следующий предмет с начала очереди
func (this *Queue) Dequeue() *Key {
	if this.Length == 0 {
		return nil
	}
	n := this.Start
	if this.Length == 1 {
		this.Start = nil
		this.End = nil
	} else {
		this.Start = this.Start.Next
	}
	this.Length--
	return n.Value
}

// Добавление элемента к хвосту очереди
func (this *Queue) Enqueue(value *Key) {
	n := &Node{value, nil}
	if this.Length == 0 {
		this.Start = n
		this.End = n
	} else {
		this.End.Next = n
		this.End = n
	}
	this.Length++
}

// Число элементов в очереди
func (this *Queue) Len() int {
	return this.Length
}

// Первый элемент в очереди без его удаления
func (this *Queue) Peek() *Node {
	if this.Length == 0 {
		return nil
	}
	return this.Start
}

// Пометить как удаленный

func (this *Queue) Delete(hash string) bool {

	fn := func(v_tmp *Node, hash string) bool {
		if v_tmp.Value.hash == hash {
			v_tmp.Value.isDeleted = true
			return true
		} else {
			return false
		}
	}

	if this.Len() == 0 {
		return false
	}
	v_tmp := this.Peek()
	for {
		if v_tmp != nil {
			if v_tmp.Value.isDeleted {
				v_tmp = v_tmp.Next
				continue
			}
			if fn(v_tmp, hash) {
				return true
			}
			v_tmp = v_tmp.Next
			continue
		}
		break
	}
	return false
}

func (this *Queue) Update(hash string, newValue Key) bool {

	fn := func(v_tmp *Node, this *Queue,
		hash string, newValue Key) bool {
		v_tmp.Value.isDeleted = true
		this.Enqueue(
			&Key{hash: hash,
				pos:       newValue.pos,
				size:      newValue.size,
				isDeleted: false})
		return true
	}

	if this.Len() == 0 {
		return false
	}

	v_tmp := this.Peek()

	for {
		if v_tmp != nil {
			if v_tmp.Value.hash == hash {
				if v_tmp.Value.isDeleted {
					continue
				}
				return fn(v_tmp, this, hash, newValue)
			}
			v_tmp = v_tmp.Next
			continue
		}
		break
	}
	return false
}

func (this *Queue) GetKeyByHash(hash string, what_kind int) (*Key, bool) {
	// what_kind = 0  только живых
	// what_kind = 1  всех
	if this.Len() == 0 {
		return nil, false
	}
	v_tmp := this.Peek()
	for {
		if v_tmp != nil {
			if what_kind == 0 {
				if v_tmp.Value.isDeleted {
					v_tmp = v_tmp.Next
					continue
				}
			}
			if v_tmp.Value.hash == hash {
				return v_tmp.Value, true
			}
			v_tmp = v_tmp.Next
			continue
		}
		break
	}
	return nil, false
}

func (this *Queue) PrintAll() {
	if this.Len() == 0 {
		fmt.Println("Пустая очередь ")
		return
	}
	v_tmp := this.Peek()
	for {
		if v_tmp != nil {
			fmt.Printf("%s %d  %d  %v \n",
				v_tmp.Value.hash, v_tmp.Value.pos,
				v_tmp.Value.size, v_tmp.Value.isDeleted)
			v_tmp = v_tmp.Next
			continue
		}
		break
	}
}
