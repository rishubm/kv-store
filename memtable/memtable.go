package memtable

import (
	"sync"

	"github.com/rishubm/kv-store/skiplist"
)

const Tombstone = "\x00TOMBSTONE\x00"

type Memtable interface {
	Put(key string, value string)
	// Get returns (value, found, deleted).
	Get(key string) (string, bool, bool)
	Delete(key string) bool
	Size() uint64
	Iterator() skiplist.Iterator
}

type memtableImpl struct {
	mutex *sync.RWMutex
	store skiplist.SkipList
}

func (m *memtableImpl) Put(key string, value string) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.store.Put(key, value)
}

func (m *memtableImpl) Get(key string) (string, bool, bool) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	val, present := m.store.Get(key)
	if !present {
		return "", false, false
	}
	if val == Tombstone {
		return "", false, true
	}
	return val, true, false
}

func (m *memtableImpl) Delete(key string) bool {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.store.Put(key, Tombstone)
	return true
}

func (m *memtableImpl) Size() uint64 {
	return m.store.ApproximateSize()
}

func (m *memtableImpl) Iterator() skiplist.Iterator {
	return m.store.Iterator()
}

func NewMemtable() Memtable {
	return &memtableImpl{&sync.RWMutex{}, skiplist.NewSkipList(16)}
}
