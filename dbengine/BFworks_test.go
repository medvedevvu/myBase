package dbengine

import (
	"bytes"
	"encoding/gob"
	"fmt"
	utl "myBase/utl"
	"os"
	"testing"
	"unsafe"
)

func TestCheckStructSize(t *testing.T) {
	want := unsafe.Sizeof(struct {
		Hash      string
		Pos       int64
		Size      int64
		IsDeleted bool
	}{utl.AsSha256([]byte(`testing data`)),
		0, 0, false})

	temp := &Key{Hash: utl.AsSha256([]byte(`test data`)),
		Pos:       0,
		Size:      10,
		IsDeleted: false}
	got := unsafe.Sizeof(*temp)
	if want != got {
		t.Errorf(" ошибся с размером want = %d got=%d \n", want, got)
	}
}

var temp = struct {
	Hash      string
	Pos       int64
	Size      int64
	IsDeleted bool
}{utl.AsSha256([]byte(`test data test data test data`)), 0, 10, false}

func TestWrieToFileAndCheckFileSize(t *testing.T) {
	file, err := os.Create("test_idx")
	defer file.Close()
	if err != nil {
		t.Errorf(" не создал временнный файл %s \n", err)
	}
	/* проверили размер пустого*/
	fi, err := file.Stat()
	if err != nil {
		t.Errorf("ошибка получения статистики файла %s \n", err)
	}
	var bin_buf bytes.Buffer
	enc := gob.NewEncoder(&bin_buf)

	err = enc.Encode(&temp)
	if err != nil {
		t.Errorf("encode error: %s", err)
	}

	n, err := file.Write(bin_buf.Bytes())
	if err != nil || n == 0 {
		t.Errorf(" не смогли записать %s  в файл %d \n", err, n)
	}
	fi1, err := file.Stat()
	if fi1.Size() != int64(n) {
		t.Errorf(" пустой = %d полный=%d образец=%d ", fi.Size(),
			fi1.Size(), int64(n))
	}

}

func TestReadFromFileAndCheckData(t *testing.T) {
	file, err := os.Create("test_idx")
	defer file.Close()
	if err != nil {
		t.Errorf(" не создал временнный файл %s \n", err)
	}

	var bin_buf bytes.Buffer
	enc := gob.NewEncoder(&bin_buf)
	err = enc.Encode(&temp)
	if err != nil {
		t.Errorf("encode error: %s", err)
	}

	n, err := file.Write(bin_buf.Bytes())
	if err != nil || n == 0 {
		t.Errorf(" не смогли записать %s  в файл %d байт \n", err, n)
	}

	ret, err := file.Seek(0, 0)
	if err != nil {
		t.Errorf(" не встали на начало %s  в файл %d байт \n", err, ret)
	}

	var bout_buf bytes.Buffer
	tf := make([]byte, n)
	n, err = file.Read(tf)
	if err != nil || n == 0 {
		t.Errorf(" не смогли прочитать %s из файла %d байт \n", err, n)
	}
	n, err = bout_buf.Write(tf)
	if err != nil || n == 0 {
		t.Errorf(" не смогли прочитать %s в буфер %d байт \n", err, n)
	}

	dec := gob.NewDecoder(&bout_buf)

	var v Key
	err = dec.Decode(&v)
	if err != nil {
		t.Errorf(" decode error %s :", err)
	}

	if temp.Hash != v.Hash ||
		temp.Pos != v.Pos ||
		temp.Size != v.Size || temp.IsDeleted != v.IsDeleted {
		t.Errorf(" want=%v не равен got= %v \n", temp, v)
	}

	fmt.Printf(" want=%v не равен got= %v \n", temp, v)
}
