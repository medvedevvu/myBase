package dbengine

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
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
}{utl.AsSha256([]byte(`data`)), 110, 9, false}

func TestWrieToFileAndCheckFileSize(t *testing.T) {
	want := WorkingDir + FlSep + "test2"
	_ = utl.ClearFiles(want)
	file, err := os.Create(want)
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

	l_bef := len(bin_buf.Bytes())
	err = utl.Set4ByteRange(&bin_buf)
	if err != nil {
		t.Errorf(" ошибка вырвнивания %s \n", err)
	}
	l_after := len(bin_buf.Bytes())

	if l_bef == l_after {
		t.Errorf(" BEFORE  %d  AFTER %d \n", l_bef, l_after)
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
	want := WorkingDir + FlSep + "test3"
	_ = utl.ClearFiles(want)
	file, err := os.Create(want)
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

	err = utl.Set4ByteRange(&bin_buf)
	if err != nil {
		t.Errorf(" ошибка вырвнивания %s \n", err)
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
		temp.Size != v.Size ||
		temp.IsDeleted != v.IsDeleted {
		t.Errorf(" want=%v не равен got= %v \n", temp, v)
	}
	fmt.Printf(" want=%v \n got= %v \n", temp, v)
}

func TestWriteBigDataAndReadIt(t *testing.T) {
	want := WorkingDir + FlSep + "test4"
	_ = utl.ClearFiles(want)
	file, err := os.Create(want)
	defer file.Close()
	if err != nil {
		t.Errorf(" не создал временнный файл %s \n", err)
	}
	for i := 0; i < 10; i++ {
		vPos := i*10 + 1
		vSize := i*99 + 41
		adds := fmt.Sprintf("%d%d", vPos, vSize)
		value := []byte(`test`)
		value = append(value, adds...)
		temp := Key{utl.AsSha256(value), int64(vPos), int64(vSize), false}
		n, err := WriteDataToFile(file, temp)
		if err != nil {
			t.Errorf("ошибка %s записи в файл - байты %d \n", err, n)
		}
	}
	// ----- попробуем искать
	/*vPos := 4*10 + 1
	vSize := 4*99 + 41
	adds := fmt.Sprintf("%d%d", vPos, vSize)
	value := []byte(`test`)
	value = append(value, adds...)
	sKey := Key{utl.AsSha256(value), int64(vPos), int64(vSize), false}
	*/
	var pos int64 = 0
	var delta int64 = 4
	file.Seek(0, 0)
	for {
		n, err := file.Seek(pos, 0)
		if err != nil {
			t.Errorf("ошибка %s позиционирования %d \n", err, n)
		}

		fmt.Printf(" pos=%d delta %d \n", pos, delta)

		var bout_buf bytes.Buffer
		tf := make([]byte, delta)
		n1, err := file.Read(tf)

		if err != nil || n1 == 0 {
			if err == io.EOF {
				break
			}
			t.Errorf(" не смогли прочитать %s из файла %d байт \n", err, n1)
		}

		n1, err = bout_buf.Write(tf)
		if err != nil || n1 == 0 {
			t.Errorf(" не смогли прочитать %s в буфер %d байт \n", err, n1)
		}

		dec := gob.NewDecoder(&bout_buf)
		var v Key
		err = dec.Decode(&v)
		if err != nil {
			t.Errorf(" decode error %s :", err)
		}

		// ----- следующие 4 байта
		pos += delta
	}

}
