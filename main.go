package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/apple/foundationdb/bindings/go/src/fdb"

	"github.com/meln1k/buse-go/buse"
	"github.com/meln1k/foundationdb-block-device/fdbarray"

	"github.com/urfave/cli"
)

// This device is an example implementation of an in-memory block device with high latency of reads/writes

type FdbStorage struct {
	array fdbarray.FDBArray
}

func CreateStorageVolume(database fdb.Database, name string, blockSize uint32, size uint64) FdbStorage {
	array := fdbarray.Create(database, name, blockSize, size)
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

func (d FdbStorage) Size() uint64 {
	return d.array.Size()
}

// func usage() {
// 	fmt.Fprintf(os.Stderr, "usage: %s /dev/nbd0\n", os.Args[0])
// 	flag.PrintDefaults()
// 	os.Exit(2)
// }

func main() {

	app := cli.NewApp()
	app.Name = "fdbbd"
	app.Version = "0.1.0"
	app.Usage = `block device using FoundationDB as a backend. 
   Our motto: still more performant and reliable than EBS`

	app.Commands = []cli.Command{
		{
			Name:      "create",
			Aliases:   []string{"c"},
			Usage:     "Create a new volume",
			ArgsUsage: "[volume name]",
			Flags: []cli.Flag{
				cli.IntFlag{
					Name:  "bs, blockSize",
					Usage: "size of a single block in bytes, must be a power of 2 and not more than 65536",
					Value: 4096,
				},
				cli.Uint64Flag{
					Name:  "s, size",
					Usage: "size of the volume in megabytes",
					Value: 512,
				},
			},
			Action: func(c *cli.Context) error {
				if !c.Args().Present() {
					return cli.NewExitError("volume name must me specified", 1)
				}

				blockSize := c.Int("blockSize")
				allowedBlockSizes := map[int]bool{
					512:   true,
					1024:  true,
					2048:  true,
					4096:  true,
					8192:  true,
					16384: true,
					32768: true,
					65536: true,
				}

				_, blockSizeValid := allowedBlockSizes[blockSize]

				if !blockSizeValid {
					return cli.NewExitError("blockSize must be a power of 2 but no more than 65536", 1)
				}

				fdb.MustAPIVersion(600)
				db := fdb.MustOpenDefault()
				size := (c.Uint64("size") * 1024 * 1024)
				name := c.Args().Get(0)
				CreateStorageVolume(db, name, uint32(blockSize), size)
				return nil
			},
		},
		{
			Name:      "open",
			Aliases:   []string{"o"},
			Usage:     "opens the volume at the provided device",
			ArgsUsage: "[volume name] [device name]",
			Action: func(c *cli.Context) error {
				if c.NArg() != 2 {
					return cli.NewExitError("volume name and device must me specified", 1)
				}
				volumeName := c.Args().Get(0)
				blockDeviceName := c.Args().Get(1)

				fdb.MustAPIVersion(600)

				// Open the default database from the system cluster
				db := fdb.MustOpenDefault()

				deviceExp := OpenStorageVolume(db, volumeName)

				device, err := buse.CreateDevice(blockDeviceName, deviceExp.Size(), deviceExp)
				if err != nil {
					fmt.Printf("Cannot create device: %s\n", err)
					os.Exit(1)
				}
				sig := make(chan os.Signal)
				signal.Notify(sig, os.Interrupt)
				fmt.Println("Waiting for SIGINT...")
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

				return nil
			},
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}

	// flag.Usage = usage
	// flag.Parse()
	// args := flag.Args()
	// if len(args) < 1 {
	// 	usage()
	// }

	if false {
		// Different API versions may expose different runtime behaviors.
		fdb.MustAPIVersion(600)

		// Open the default database from the system cluster
		db := fdb.MustOpenDefault()
		size := uint64(1024 * 1024 * 1024) // 1024
		deviceExp := CreateStorageVolume(db, "nbdftw", 4096, size)
		device, err := buse.CreateDevice("/dev/nbd0", size, deviceExp)
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
}
