package sstable

import (
	"container/heap"
	"encoding/binary"
	"io"
	"os"
	"slices"
	"strconv"
	"time"

	"github.com/rishubm/kv-store/keyheap"
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

// Merge performs a k-way merge on the sstable file names provided in tables
// Efficient merge, O(N*log(k))
func Merge(tables []string, dir string) (string, error) {
	sorted := tables
	slices.Sort(sorted)

	// Open each file
	files := make([]*os.File, 0, len(sorted))
	for _, path := range sorted {
		file, err := os.Open(path)
		if err != nil {
			return "", err
		}
		files = append(files, file)
		defer file.Close()
	}

	// Seed key heap
	kheap := keyheap.NewKeyHeap()
	for i, f := range files {
		_, err, key, val := readKVPair(f)
		if err != nil {
			return "", err
		}
		node := keyheap.HeapNode{Key: key, Value: val, FileIndex: i}
		kheap.Push(node)
	}
	heap.Init(&kheap)

	// Main merge loop w/ min heap
	merged := make([]keyheap.HeapNode, 0)
	prevKey := ""
	for kheap.Len() > 0 {
		popped := heap.Pop(&kheap).(keyheap.HeapNode)
		// Advance the file for the popped key
		done, err, key, val := readKVPair(files[popped.FileIndex])
		if err != nil {
			return "", err
		}
		if !done {
			heap.Push(&kheap, keyheap.HeapNode{Key: key, Value: val, FileIndex: popped.FileIndex})
		}
		// duplicate key, skip
		if popped.Key == prevKey || popped.Value == Tombstone {
			prevKey = popped.Key
			continue
		}
		merged = append(merged, popped)
		prevKey = popped.Key

	}
	// Write merged to a file now
	fileName := "data-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	path := dir + fileName
	out, err := os.Create(path)
	if err != nil {
		return "", err
	}
	defer out.Close()
	for _, node := range merged {
		keyBytes, valBytes := []byte(node.Key), []byte(node.Value)
		binary.Write(out, binary.LittleEndian, uint32(len(keyBytes)))
		out.Write(keyBytes)
		binary.Write(out, binary.LittleEndian, uint32(len(valBytes)))
		out.Write(valBytes)
	}
	out.Sync()
	return path, nil
}

// Reads the next single KV pair from a file
func readKVPair(file *os.File) (done bool, err error, key string, val string) {
	var keyLen uint32
	err = binary.Read(file, binary.LittleEndian, &keyLen)
	if err == io.EOF {
		return true, nil, "", ""
	}
	if err != nil {
		return false, err, "", ""
	}
	keyBytes := make([]byte, keyLen)
	io.ReadFull(file, keyBytes)

	var valLen uint32
	binary.Read(file, binary.LittleEndian, &valLen)

	valBytes := make([]byte, valLen)
	io.ReadFull(file, valBytes)
	return false, nil, string(keyBytes), string(valBytes)
}
