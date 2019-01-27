package fdbarray

import (
	"bytes"
	"math/rand"
	"os"
	"testing"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

var fdbArray FDBArray

func setup() {

	fdb.MustAPIVersion(600)

	// Open the default database from the system cluster
	db := fdb.MustOpenDefault()

	fdbArray = Create(db, "fdbarray-test", 512)
}

func cleanup() {
	fdbArray.Delete()
}

func clearArray() {
	fdbArray.Clear()
}

func TestNotAlignedReadWrite(t *testing.T) {
	write := make([]byte, 12345)
	for i := 0; i < 12345; i++ {
		write[i] = byte(i)
	}

	fdbArray.Write(write, 10000)

	read := make([]byte, 12345)

	fdbArray.Read(read, 2)

	if !bytes.Equal(write, read) {
		t.Errorf("Write is not equal to read")
	}
}

func TestRandomReadWrite(t *testing.T) {

	rand.Seed(42)

	for i := 0; i < 100; i++ {
		go func() {
			length := rand.Int31n(1000)
			write := make([]byte, length)
			rand.Read(write)
			offset := uint64(rand.Int63n(1000000))
			fdbArray.Write(write, offset)
			read := make([]byte, length)
			fdbArray.Read(read, offset)
			if !bytes.Equal(write, read) {
				t.Errorf("Write is not equal to read!")
			}
		}()
	}

}

func TestMain(m *testing.M) {
	setup()
	res := m.Run()
	cleanup()
	os.Exit(res)
}
