package engine

import (
	"os"
	"slices"
	"sync"

	"github.com/rishubm/kv-store/memtable"
	"github.com/rishubm/kv-store/sstable"
	"github.com/rishubm/kv-store/wal"
)

type Engine interface {
	Put(key string, value string)
	Get(key string) (string, bool)
	Delete(key string) bool
}

type engineImpl struct {
	activeMem          memtable.Memtable
	immMem             memtable.Memtable
	flushChan          chan memtable.Memtable
	compactChan        chan struct{}
	activeMemThreshold uint64
	mutex              *sync.RWMutex
	sstables           []string
	cond               *sync.Cond
}

// Maximum number of SSTables before merging
const maxSSTableCount uint8 = 4

func (e *engineImpl) Put(key string, value string) {
	e.mutex.Lock()
	err := wal.AppendLog(wal.Put, key, value)
	if err != nil {
		panic(err)
	}
	e.activeMem.Put(key, value)
	reachedThreshold := e.activeMem.Size() > e.activeMemThreshold
	e.mutex.Unlock()

	if reachedThreshold {
		// lock acquired = guarantee no goroutines are currently writing to the active memtable
		e.mutex.Lock()
		// Double check as another goroutine might have already swapped
		if e.activeMem.Size() > e.activeMemThreshold {

			// wait on the cond var to avoid overwriting immMem before it's flushed
			for e.immMem != nil {
				e.cond.Wait()
			}
			// copy the. current active memtable to the immutable table
			e.immMem = e.activeMem
			old := e.immMem
			// make a fresh memtable
			e.activeMem = memtable.NewMemtable()
			e.mutex.Unlock()
			// send the old (immutable) memtable to get flushed to disk
			e.flushChan <- old
			return
		}
		e.mutex.Unlock()
	}

}

func (e *engineImpl) Get(key string) (string, bool) {
	// First check active memtable
	e.mutex.RLock()
	val, present := e.activeMem.Get(key)
	if present {
		e.mutex.RUnlock()
		return val, true
	}
	e.mutex.RUnlock()

	// check immutable memtable to see if being flushed to disk
	e.mutex.RLock()
	if e.immMem != nil {
		val, present = e.immMem.Get(key)
		if present {
			e.mutex.RUnlock()
			return val, true
		}
	}
	e.mutex.RUnlock()

	// otherwise check sstable in decsending time order
	e.mutex.RLock()
	sorted := e.sstables
	e.mutex.RUnlock()
	slices.Sort(sorted)
	slices.Reverse(sorted)

	for _, path := range sorted {
		val, present, err := sstable.Read(path, key)
		if err != nil {
			panic(err.Error())
		}
		if present {
			return val, true
		}
	}
	return "", false
}

func (e *engineImpl) Delete(key string) bool {
	e.mutex.Lock()
	defer e.mutex.Unlock()
	err := wal.AppendLog(wal.Delete, key, "")
	if err != nil {
		panic(err)
	}
	return e.activeMem.Delete(key)
}

// Flushes the immutable memtable to an sstable on disk
func (e *engineImpl) flushWorker() {

	// wait for the old memtable from the channel
	for mem := range e.flushChan {
		path, err := sstable.Write(mem, "./data/")
		if err != nil {
			panic("failed to flush memtable to disk" + err.Error())
		}

		e.mutex.Lock()

		e.immMem = nil
		
		// signal that it's okay to overwrite the immutable memtable
		e.cond.Signal()
		e.sstables = append(e.sstables, path)
		e.mutex.Unlock()

		// Trigger compaction if needed
		select {
		// send to the channel, telling it's ready to compact
		case e.compactChan <- struct{}{}:
		// otherwise return/skip since the channel is busy
		default:
		}
	}

}

// Compacts sstables on disk using merge
func (e *engineImpl) compactWorker() {
	for range e.compactChan {
		// Check if we actually need to compact
		e.mutex.RLock()
		var snapshot []string = nil
		if len(e.sstables) >= int(maxSSTableCount) {
			snapshot = slices.Clone(e.sstables)
		}
		e.mutex.RUnlock()

		if snapshot != nil {
			newFile, err := sstable.Merge(snapshot, "./data/")
			if err != nil {
				panic(err.Error())
			}

			// write the new file
			e.mutex.Lock()
			e.sstables = append(e.sstables, newFile)
			e.sstables = e.sstables[len(snapshot):]
			e.mutex.Unlock()

			// safe to delete the old files since they are no longer referenced
			for _, path := range snapshot {
				os.Remove(path)
			}
		}

	}

}

func NewEngine(threshold uint64) Engine {
	activeMem := memtable.NewMemtable()
	flushChannel := make(chan memtable.Memtable, 1)
	comapctChannel := make(chan struct{}, 1)
	sstables := make([]string, 0)
	mu := &sync.RWMutex{}
	e := &engineImpl{activeMem, nil, flushChannel, comapctChannel, threshold, mu, sstables, sync.NewCond(mu)}

	// Replay the WAL for any lost changes
	entries, _ := wal.ReplayLog()
	for _, entry := range entries {
		if entry.Op == wal.Put {
			e.activeMem.Put(entry.Key, entry.Value)
		} else {
			e.activeMem.Delete(entry.Key)
		}
	}
	// start the flush and comapct worker goroutines
	go e.flushWorker()
	go e.compactWorker()
	return e
}
