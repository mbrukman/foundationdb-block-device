package main

import (
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

func main() {
	// Different API versions may expose different runtime behaviors.
	fdb.MustAPIVersion(600)

	// Open the default database from the system cluster
	db := fdb.MustOpenDefault()

	// Database reads and writes happen inside transactions
	ret, e := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		tr.Set(fdb.Key("hello"), []byte("world"))
		fooVal := tr.Get(fdb.Key("second")).MustGet()
		newFooVal := time.Now().Second()
		tr.Set(fdb.Key("second"), []byte(strconv.Itoa(newFooVal)))
		// db.Transact automatically commits (and if necessary,
		// retries) the transaction
		return fooVal, nil
	})
	if e != nil {
		log.Fatalf("Unable to perform FDB transaction (%v)", e)
	}

	fmt.Printf("hello is now world, second was: %s\n", string(ret.([]byte)))
}
