package sstable

import (
	"encoding/binary"
	"io"
	"os"
	"strconv"
	"time"

	"github.com/rishubm/kv-store/memtable"
)

const Tombstone = "\x00TOMBSTONE\x00"

// Write serializes a memtable to an SSTable file on disk in sorted key order.
// dir is the directory where the file will be created.
// Returns the path to the created file, or an error.
func Write(mem memtable.Memtable, dir string) (string, error) {
	fileName := "data-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	path := dir + fileName
	file, err := os.Create(path)
	if err != nil {
		return "", err
	}
	defer file.Close()

	iter := mem.Iterator()
	for iter.Next() {
		key, val := iter.Key(), iter.Value()
		keyBytes, valBytes := []byte(key), []byte(val)
		binary.Write(file, binary.LittleEndian, uint32(len(keyBytes)))
		file.Write(keyBytes)
		binary.Write(file, binary.LittleEndian, uint32(len(valBytes)))
		file.Write(valBytes)
	}
	file.Sync()
	return path, nil
}

// Read scans an SSTable file for a given key.
// Returns the value and true if found, or "", false if not present.
func Read(path string, key string) (string, bool, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", false, err
	}
	defer file.Close()

	for {
		var keyLen uint32
		err := binary.Read(file, binary.LittleEndian, &keyLen)
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", false, err
		}
		keyBytes := make([]byte, keyLen)
		io.ReadFull(file, keyBytes)

		var valLen uint32
		binary.Read(file, binary.LittleEndian, &valLen)

		valBytes := make([]byte, valLen)
		io.ReadFull(file, valBytes)

		if string(keyBytes) == key && string(valBytes) != Tombstone {
			return string(valBytes), true, nil
		}
	}
	return "", false, nil
}
