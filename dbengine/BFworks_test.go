package dbengine

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	utl "myBase/utl"
	"os"
	"testing"
	"unsafe"
)

var want uintptr = unsafe.Sizeof(struct {
	hash      string
	pos       int64
	size      int64
	isDeleted bool
}{utl.AsSha256([]byte(`testing data`)),
	0, 0, false})

func TestCheckStructSize(t *testing.T) {
	temp := &Key{hash: utl.AsSha256([]byte(`test data`)),
		pos:       0,
		size:      10,
		isDeleted: false}
	got := unsafe.Sizeof(*temp)
	if want != got {
		t.Errorf(" ошибся с размером want = %d got=%d \n", want, got)
	}
}

func TestWrieToFileAndCheckFileSize(t *testing.T) {
	temp := struct {
		hash      string
		pos       int64
		size      int64
		isDeleted bool
	}{utl.AsSha256([]byte(`test data`)), 0, 10, false}
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
	sz := binary.Size(temp)
	fmt.Printf(" size %d \n", sz)
	err = binary.Write(&bin_buf, binary.BigEndian, temp)

	if err != nil {
		t.Errorf(" не смогли записать байты в буфер %s \n", err)
	}

	n, err := file.Write(bin_buf.Bytes())
	if err != nil || n == 0 {
		t.Errorf(" не смогли записать %s  в файл %d \n", err, n)
	}
	fi1, err := file.Stat()
	if fi1.Size() != int64(want) {
		t.Errorf(" пустой = %d полный=%d образец=%d ", fi.Size(),
			fi1.Size(), int64(want))
	}
}

func writeNextBytes(file *os.File, bytes []byte) {

	_, err := file.Write(bytes)

	if err != nil {
		log.Fatal(err)
	}

}
