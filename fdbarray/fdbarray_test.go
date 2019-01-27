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

	fdbArray = Create(db, "fdbarray-test", 8)
}

func cleanup() {
	fdbArray.Delete()
}

func clearArray() {
	fdbArray.Clear()
}

func TestNotAlignedReadWrite(t *testing.T) {
	write := make([]byte, 36)
	for i := 0; i < 36; i++ {
		write[i] = byte(i)
	}

	fdbArray.Write(write, 2)

	read := make([]byte, 36)

	fdbArray.Read(read, 2)

	if !bytes.Equal(write, read) {
		t.Errorf("Write is not equal to read")
	}
}

func TestRandomReadWrite(t *testing.T) {

	rand.Seed(42)

	for i := 0; i < 1; i++ {
		length := rand.Int31n(256)
		write := make([]byte, length)
		rand.Read(write)
		offset := uint64(rand.Int63n(100000))
		fdbArray.Write(write, offset)
		read := make([]byte, length)
		fdbArray.Read(read, offset)
		if !bytes.Equal(write, read) {
			t.Errorf("Write is not equal to read!")
		}
	}

}

func TestMain(m *testing.M) {
	setup()
	res := m.Run()
	cleanup()
	os.Exit(res)
}
