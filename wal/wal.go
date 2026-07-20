package wal

import (
	"encoding/binary"
	"io"
	"os"
)

const wal_name string = "../wal.log"

type Opcode uint8

// Put = 0, Delete = 1
const (
	Put Opcode = iota
	Delete
)

type LogEntry struct {
	Op    Opcode
	Key   string
	Value string
}

func AppendLog(op Opcode, k string, v string) error {
	file, err := os.OpenFile(wal_name, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
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

func ReplayLog() ([]LogEntry, error) {
	if _, err := os.Stat(wal_name); os.IsNotExist(err) {
		// log doesn't exist, first run not an error
		return []LogEntry{}, nil
	}
	file, err := os.Open(wal_name)
	if err != nil {
		return []LogEntry{}, err
	}
	defer file.Close()
	entries := make([]LogEntry, 0)

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
	return entries, nil
}
