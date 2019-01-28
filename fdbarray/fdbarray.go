package fdbarray

import (
	"encoding/binary"
	"log"
	"math"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

const (
	FDBArrayDirectoryName = "com.github.meln1k.fdb-nbd"
	MetadataDirectoryName = "metadata"
	DataDirectoryName     = "data"
	BlockSizeKey          = "bs"
	SizeKey               = "size"
)

type FDBArray struct {
	database fdb.Database
	subspace directory.DirectorySubspace
	metadata directory.DirectorySubspace
	data     directory.DirectorySubspace

	blockSize uint32
	size      uint64
}

// Create a new array
// database - instance of the database
// name - name of the array
// blockSize - size of the block in bytes
// size - size of the volume in bytes
func Create(database fdb.Database, name string, blockSize uint32, size uint64) FDBArray {
	subspace, err := directory.Create(database, []string{FDBArrayDirectoryName, name}, nil)
	if err != nil {
		log.Fatal(err)
	}

	metadata, err := subspace.Create(database, []string{MetadataDirectoryName}, nil)
	if err != nil {
		log.Fatal(err)
	}

	data, err := subspace.Create(database, []string{DataDirectoryName}, nil)
	if err != nil {
		log.Fatal(err)
	}

	_, err = database.Transact(func(tr fdb.Transaction) (ret interface{}, err error) {
		bs := make([]byte, 4)
		binary.BigEndian.PutUint32(bs, blockSize)
		tr.Set(metadata.Pack(tuple.Tuple{BlockSizeKey}), bs)

		s := make([]byte, 8)
		binary.BigEndian.PutUint64(s, size)
		tr.Set(metadata.Pack(tuple.Tuple{SizeKey}), s)
		return
	})
	if err != nil {
		log.Fatal(err)
	}

	fdbArray := FDBArray{database, subspace, metadata, data, blockSize, size}

	return fdbArray
}

// Open an already created array
func Open(database fdb.Database, name string) FDBArray {
	subspace, err := directory.Open(database, []string{FDBArrayDirectoryName, name}, nil)
	if err != nil {
		log.Fatal(err)
	}

	metadata, err := subspace.CreateOrOpen(database, []string{MetadataDirectoryName}, nil)
	if err != nil {
		log.Fatal(err)
	}

	data, err := subspace.CreateOrOpen(database, []string{DataDirectoryName}, nil)
	if err != nil {
		log.Fatal(err)
	}

	blockSize, err := database.Transact(func(tr fdb.Transaction) (ret interface{}, err error) {
		bytes := tr.Get(metadata.Pack(tuple.Tuple{BlockSizeKey})).MustGet()
		return binary.BigEndian.Uint32(bytes), nil
	})

	size, err := database.Transact(func(tr fdb.Transaction) (ret interface{}, err error) {
		bytes := tr.Get(metadata.Pack(tuple.Tuple{SizeKey})).MustGet()
		return binary.BigEndian.Uint64(bytes), nil
	})

	fdbArray := FDBArray{database, subspace, metadata, data, blockSize.(uint32), size.(uint64)}

	return fdbArray
}

func (array FDBArray) readSingleBlockAsync(blockID uint64, tx fdb.ReadTransaction) fdb.FutureByteSlice {
	return tx.Get(array.data.Pack(tuple.Tuple{blockID}))
}

func (array FDBArray) Read(read []byte, offset uint64) error {
	blockSize := uint64(array.blockSize)
	firstBlock := offset / blockSize
	blockOffset := (offset % blockSize)
	length := uint64(len(read))
	lastBlock := (offset + length) / blockSize

	_, err := array.database.Transact(func(tx fdb.Transaction) (ret interface{}, err error) {

		if length == blockSize && blockOffset == 0 {
			value := array.readSingleBlockAsync(firstBlock, tx).MustGet()

			copy(read, value)

		} else {
			iterator := tx.GetRange(
				fdb.KeyRange{Begin: array.data.Pack(tuple.Tuple{firstBlock}), End: array.data.Pack(tuple.Tuple{lastBlock + 1})},
				fdb.RangeOptions{Limit: 0, Mode: fdb.StreamingModeWantAll, Reverse: false}).Iterator()

			for iterator.Advance() {
				kv := iterator.MustGet()

				tuple, err := array.data.Unpack(kv.Key)
				if err != nil {
					log.Fatal(err)
					return nil, err
				}

				blockID := uint64(tuple[0].(int64))
				buffer := make([]byte, blockSize)
				copy(buffer, kv.Value)
				copyBlock(read, firstBlock, blockOffset, lastBlock, buffer, blockID, blockSize)
			}

		}
		return
	})

	if err != nil {
		return err
	}
	return nil
}

func copyBlock(read []byte, firstBlock uint64, blockOffset uint64, lastBlock uint64, currentValue []byte, blockID uint64, blockSize uint64) {

	blockPosition := (blockID - firstBlock) * blockSize
	shift := blockSize - blockOffset
	if blockID == firstBlock {
		firstBlockLength := uint64(math.Min(float64(shift), float64(len(read))))
		copy(read[0:firstBlockLength], currentValue[blockOffset:blockOffset+firstBlockLength])
	} else {
		position := int(blockPosition - blockSize + shift)
		if blockID == lastBlock {
			lastLength := len(read) - position
			copy(read[position:position+lastLength], currentValue[0:lastLength])
		} else {
			copy(read[position:position+int(blockSize)], currentValue[0:int(blockSize)])
		}
	}
}

func isFirstBlockPartial(blockOffset uint64, length uint64, blockSize uint64) bool {
	return blockOffset > 0 || (blockOffset == 0 && length < blockSize)
}

func isLastBlockPartial(lastBlock uint64, firstBlock uint64, length uint64, blockSize uint64, shift uint64) bool {
	if lastBlock > firstBlock {
		position := ((lastBlock-firstBlock-1)*blockSize + shift)
		lastBlockLength := length - position
		return lastBlockLength != blockSize
	}
	return false
}

func (array FDBArray) Write(write []byte, offset uint64) error {
	blockSize := uint64(array.blockSize)
	length := uint64(len(write))
	firstBlock := offset / blockSize
	lastBlock := (offset + length) / blockSize
	blockOffset := (offset % blockSize)
	shift := blockSize - blockOffset

	array.database.Transact(func(tx fdb.Transaction) (ret interface{}, err error) {

		firstBlockKey := array.data.Pack(tuple.Tuple{firstBlock})

		// Prefetch last and first blocks in parallel if needed to reduce overall latency
		var maybeFirstBlockRead fdb.FutureByteSlice
		if isFirstBlockPartial(blockOffset, length, blockSize) {
			maybeFirstBlockRead = array.readSingleBlockAsync(firstBlock, tx)
		}

		var maybeLastBlockRead fdb.FutureByteSlice
		if isLastBlockPartial(lastBlock, firstBlock, length, blockSize, shift) {
			maybeLastBlockRead = array.readSingleBlockAsync(lastBlock, tx)
		}

		// While first and last blocks are being fetched, let's set the middle blocks
		if lastBlock > firstBlock {
			// blocks in the middle
			for i := firstBlock + 1; i < lastBlock; i++ {
				key := array.data.Pack(tuple.Tuple{i})
				writeBlock := i - firstBlock
				position := (writeBlock-1)*blockSize + shift
				tx.Set(key, write[position:position+blockSize])
			}

			position := ((lastBlock-firstBlock-1)*blockSize + shift)
			lastBlockLength := length - position
			lastBlockKey := array.data.Pack(tuple.Tuple{lastBlock})
			// If the last block is a complete block we don't need to read
			if lastBlockLength == blockSize {
				tx.Set(lastBlockKey, write[position:position+blockSize])
			} else {
				lastBlockBytes := maybeLastBlockRead.MustGet()
				buf := make([]byte, blockSize)
				copy(buf, lastBlockBytes)
				copy(buf, write[position:position+lastBlockLength])
				tx.Set(lastBlockKey, buf)
			}
		}

		// first block should be fetched by now, let's set it too
		if isFirstBlockPartial(blockOffset, length, blockSize) {
			// Only need to do this if the first block is partial
			readBytes := maybeFirstBlockRead.MustGet()
			buf := make([]byte, blockSize)
			copy(buf, readBytes)
			writeLength := uint64(math.Min(float64(length), float64(shift)))
			copy(buf[blockOffset:blockOffset+writeLength], write[0:writeLength])
			tx.Set(firstBlockKey, buf)
		} else {
			// In this case copy the full first block blindly
			tx.Set(firstBlockKey, write[0:blockSize])
		}

		return
	})

	return nil
}

// Clear the array
func (array FDBArray) Clear() {
	array.database.Transact(func(tx fdb.Transaction) (ret interface{}, err error) {
		tx.ClearRange(array.data)
		return
	})
}

// Usage of the array
func (array FDBArray) Usage() (uint64, error) {

	// TODO implement with roaring bitset

	return 0, nil
}

// Delete the array
func (array FDBArray) Delete() {
	array.subspace.Remove(array.database, nil)
}

// Size of the volume in bytes
func (array FDBArray) Size() uint64 {
	return array.size
}