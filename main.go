package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/apple/foundationdb/bindings/go/src/fdb"

	"github.com/meln1k/buse-go/buse"
	"github.com/meln1k/foundationdb-block-device/fdbarray"
)

// This device is an example implementation of an in-memory block device with high latency of reads/writes

type FdbStorage struct {
	array fdbarray.FDBArray
}

func CreateStorageVolume(database fdb.Database, name string, blockSize uint32) FdbStorage {
	array := fdbarray.Create(database, name, blockSize)
	return FdbStorage{array: array}
}

func OpenStorageVolume(database fdb.Database, name string) FdbStorage {
	array := fdbarray.Open(database, name)
	return FdbStorage{array: array}
}

func (d FdbStorage) ReadAt(p []byte, off uint64) error {
	d.array.Read(p, off)
	return nil
}

func (d FdbStorage) WriteAt(p []byte, off uint64) error {
	d.array.Write(p, off)
	return nil
}

func (d FdbStorage) Disconnect() {
	log.Println("[DeviceExample] DISCONNECT")
}

func (d FdbStorage) Flush() error {
	log.Println("[DeviceExample] FLUSH")
	return nil
}

func (d FdbStorage) Trim(off uint64, length uint32) error {
	log.Printf("[DeviceExample] TRIM offset:%d len:%d\n", off, length)
	return nil
}

func usage() {
	fmt.Fprintf(os.Stderr, "usage: %s /dev/nbd0\n", os.Args[0])
	flag.PrintDefaults()
	os.Exit(2)
}

func main() {
	flag.Usage = usage
	flag.Parse()
	args := flag.Args()
	if len(args) < 1 {
		usage()
	}
	// Different API versions may expose different runtime behaviors.
	fdb.MustAPIVersion(600)

	// Open the default database from the system cluster
	db := fdb.MustOpenDefault()
	size := uint64(1024 * 1024 * 1024) // 1024
	deviceExp := OpenStorageVolume(db, "nbdftw")
	device, err := buse.CreateDevice(args[0], size, deviceExp)
	if err != nil {
		fmt.Printf("Cannot create device: %s\n", err)
		os.Exit(1)
	}
	sig := make(chan os.Signal)
	signal.Notify(sig, os.Interrupt)
	go func() {
		if err := device.Connect(); err != nil {
			log.Printf("Buse device stopped with error: %s", err)
		} else {
			log.Println("Buse device stopped gracefully.")
		}
	}()
	<-sig
	// Received SIGTERM, cleanup
	fmt.Println("SIGINT, disconnecting...")
	device.Disconnect()
}
