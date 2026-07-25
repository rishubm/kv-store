package wal

import (
	"encoding/binary"
	"io"
	"os"
	"slices"
	"strconv"
	"time"
)

const wal_dir string = "../wal-data/"

type Opcode uint8

// Put = 0, Delete = 1
const (
	Put Opcode = iota
	Delete
)

type WalWrapper struct {
	// the write-ahead log to write to
	wal_name string
}

type LogEntry struct {
	Op    Opcode
	Key   string
	Value string
}

func (w *WalWrapper) AppendLog(op Opcode, k string, v string) error {
	file, err := os.OpenFile(w.wal_name, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		return err
	}
	defer file.Close()
	binary.Write(file, binary.LittleEndian, op)
	keyBytes, valBytes := []byte(k), []byte(v)
	binary.Write(file, binary.LittleEndian, uint32(len(keyBytes)))
	file.Write(keyBytes)
	binary.Write(file, binary.LittleEndian, uint32(len(valBytes)))
	file.Write(valBytes)

	//imediate durability, extra overhead for blocking call
	err = file.Sync()
	if err != nil {
		return err
	}

	return nil
}

func (w *WalWrapper) ReplayLog() ([]LogEntry, error) {
	entries := make([]LogEntry, 0)
	// Read ALL write-ahead logs in the directory and append ALL entries
	if logs, err := os.ReadDir(wal_dir); err == nil {
		for _, log := range logs {
			file, err := os.Open(wal_dir + log.Name())
			if err != nil {
				return []LogEntry{}, err
			}
			defer file.Close()

			for {
				var op Opcode
				err = binary.Read(file, binary.LittleEndian, &op)
				if err == io.EOF {
					break
				}
				if err != nil {
					return []LogEntry{}, err
				}

				var keyLen uint32
				binary.Read(file, binary.LittleEndian, &keyLen)

				keyBytes := make([]byte, keyLen)
				io.ReadFull(file, keyBytes)

				var valLen uint32
				binary.Read(file, binary.LittleEndian, &valLen)

				valBytes := make([]byte, valLen)
				io.ReadFull(file, valBytes)
				entries = append(entries, LogEntry{op, string(keyBytes), string(valBytes)})

			}
		}
	}
	return entries, nil
}

func (w *WalWrapper) RotateLog() string {
	path := wal_dir + "wal" + strconv.FormatInt(time.Now().UnixNano(), 10) + ".log"
	old := w.wal_name
	w.wal_name = path
	return old

}

func NewWalWrapper() *WalWrapper {
	if err := os.MkdirAll(wal_dir, 0755); err != nil {
		panic(err)
	}
	entries, err := os.ReadDir(wal_dir)
	path := ""
	if err != nil || len(entries) == 0 {
		path = wal_dir + "wal" + strconv.FormatInt(time.Now().UnixNano(), 10) + ".log"
	} else {
		slices.Reverse(entries)
		path = wal_dir + entries[0].Name()
	}
	return &WalWrapper{path}
}
