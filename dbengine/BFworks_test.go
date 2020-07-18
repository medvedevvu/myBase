package dbengine

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	utl "myBase/utl"
	"os"
	"reflect"
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
	//	var pos int64 = 0
	var delta int64 = 4

	file, err = os.OpenFile(want, os.O_RDONLY, 0644)
	if err != nil {
		msg := fmt.Sprintf(" %s не смогли прочитать файл \n", err)
		t.Errorf(msg)
	}

	i := 0
	tf := make([]byte, delta)
	//	var bout_buf bytes.Buffer
	//	res := []byte{}

	for {
		fmt.Printf("%d   %v \n", len(tf), tf)
		n1, err := file.Read(tf)
		if err == io.EOF {
			//	достигнут конец файла
			break
		}
		if n1 == 0 || err != nil {
			msg := fmt.Sprintf("не смогли прочитать %s из файла %d байт \n", err, n1)
			t.Errorf(msg)
		}
		i++ // стчётчик итераций
		if reflect.DeepEqual(tf, []byte(`\0\0`)) {
			fmt.Printf(" stop mask  %d\n", i)
			// формируем прочитанные данные
			/*	n1, err = bout_buf.Write(res)
				if err != nil || n1 == 0 {
					t.Errorf(" не смогли прочитать %s в буфер %d байт \n", err, n1)
				}
				dec := gob.NewDecoder(&bout_buf)
				var v Key
				err = dec.Decode(&v)
				if err != nil {
					t.Errorf(" decode error %s :", err)
				}
				fmt.Printf("%v \n", v) */
			continue
		}
		cnt := utl.CountEmptyBytes(tf)
		if cnt > 0 {
			// tf = utl.CleanEmptyByte(tf)
			fmt.Printf(" last line of data  %d\n", i)
		}
		// res = append(res, tf...)
		continue // переходим к основному циклу
	} // tnd of loop
}

/*
   MainLoop:
   	for {
   		var bout_buf bytes.Buffer
   		tf := make([]byte, delta)
   		res := []byte{}
   		for {
   			//fmt.Printf(" pos=%d delta %d \n", pos, delta)
   			n1, err := file.ReadAt(tf, pos)
   			fmt.Printf(" n1=%d \n", n1)
   			if err == io.EOF {
   				break MainLoop
   			}
   			if err != nil || n1 == 0 {
   				msg := fmt.Sprintf("не смогли прочитать %s из файла %d байт \n", err, n1)
   				t.Errorf(msg)
   			}
   			//fmt.Printf("---- %v --- %v ---\n", tf, []byte(`\0\0`))
   			if reflect.DeepEqual(tf, []byte(`\0\0`)) {
   				pos += delta
   				break
   			}
   			cnt := utl.CountEmptyBytes(tf)
   			if cnt > 0 {
   				tf = utl.CleanEmptyByte(tf)
   			}
   			res = append(res, tf...)
   			pos += delta
   		}

   		n1, err := bout_buf.Write(res)
   		if err != nil || n1 == 0 {
   			t.Errorf(" не смогли прочитать %s в буфер %d байт \n", err, n1)
   		}

   		dec := gob.NewDecoder(&bout_buf)
   		var v Key
   		err = dec.Decode(&v)
   		if err != nil {
   			t.Errorf(" decode error %s :", err)
   		}
   	}
*/