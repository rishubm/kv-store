package engine

import (
	"fmt"
	"os"
	"testing"
)

// 4 MB
const threshold uint64 = 1 << 22

func TestMain(m *testing.M) {
	// create the data directory the flush worker writes to
	os.MkdirAll("./data", 0755)
	code := m.Run()
	// clean up SSTable files written during tests
	os.RemoveAll("./data")
	os.RemoveAll("../wal.log")
	os.Exit(code)
}

// Put then Get returns the correct value.
func TestPutGet(t *testing.T) {
	e := NewEngine(threshold)
	e.Put("hello", "world")

	val, ok := e.Get("hello")
	if !ok {
		t.Fatal("expected key 'hello' to be present")
	}
	if val != "world" {
		t.Fatalf("expected 'world', got %q", val)
	}
}

// Get on a missing key returns false.
func TestGetMissingKey(t *testing.T) {
	e := NewEngine(threshold)

	_, ok := e.Get("ghost")
	if ok {
		t.Fatal("expected missing key to return false")
	}
}

// A Put overwrites an earlier value for the same key.
func TestPutOverwrite(t *testing.T) {
	e := NewEngine(threshold)
	e.Put("k", "v1")
	e.Put("k", "v2")

	val, ok := e.Get("k")
	if !ok {
		t.Fatal("key should be present after overwrite")
	}
	if val != "v2" {
		t.Fatalf("expected 'v2', got %q", val)
	}
}

// Delete causes the key to no longer be found.
func TestDelete(t *testing.T) {
	e := NewEngine(threshold)
	e.Put("gone", "soon")
	e.Delete("gone")

	_, ok := e.Get("gone")
	if ok {
		t.Fatal("key should be absent after delete")
	}
}

// Multiple distinct keys can be stored and retrieved independently
func TestMultipleKeys(t *testing.T) {
	e := NewEngine(threshold)

	pairs := map[string]string{
		"a": "1",
		"b": "2",
		"c": "3",
	}
	for k, v := range pairs {
		e.Put(k, v)
	}
	for k, want := range pairs {
		got, ok := e.Get(k)
		if !ok {
			t.Errorf("key %q not found", k)
		}
		if got != want {
			t.Errorf("key %q: expected %q, got %q", k, want, got)
		}
	}
}

// Writes beyond the threshold trigger a flush; the key must still be readable
func TestFlushAndRead(t *testing.T) {
	// Use a 1KB threshold so a flush is forced quickly
	smallThreshold := uint64(1 << 10)
	e := NewEngine(smallThreshold)

	// Write enough data to exceed the threshold and trigger a flush
	for i := 0; i < 2000; i++ {
		e.Put(fmt.Sprintf("key-%d", i), fmt.Sprintf("value-%d", i))
	}

	// All keys must still be readable (from memtables or SSTables)
	for i := 0; i < 2000; i++ {
		k := fmt.Sprintf("key-%d", i)
		want := fmt.Sprintf("value-%d", i)
		got, ok := e.Get(k)
		if !ok {
			t.Errorf("key %q missing after flush", k)
			continue
		}
		if got != want {
			t.Errorf("key %q: expected %q, got %q", k, want, got)
		}
	}
}

// On reading from SStables it should read the newest values
func TestFlushGetUsesNewest(t *testing.T) {
	tinyThreshold := uint64(16)
	e := NewEngine(tinyThreshold)
	e.Put("hello", "world")
	e.Put("hello1", "world1")

	e.Put("hello", "goodbye")
	e.Put("hello1", "goodbye1")

	// fill the memtable to force a rotation
	e.Put("aaaaaaaa", "bbbbbbbb")
	e.Put("bbbbbbbb", "cccccccc")
	e.Put("cccccccc", "dddddddd")
	e.Put("eeeeeeee", "ffffffff")

	val, ok := e.Get("hello")
	if !ok {
		t.Fatal("expected key 'hello' to be present")
	}
	if val != "goodbye" {
		t.Fatalf("expected 'goodbye', got %q", val)
	}
}

func TestWriteAheadLog(t *testing.T) {
	tinyThreshold := uint64(16)
	e := NewEngine(tinyThreshold)
	e.Put("hello", "world")
	e.Put("hello1", "world1")
	e.Put("hello2", "world2")
	e.Put("deleted", "soon")
	e.Delete("deleted")
	// reopen the engine - log should replay and populate the memtable
	e = NewEngine(tinyThreshold)
	val, ok := e.Get("hello")
	if !ok {
		t.Fatal("expected key 'hello' to be present")
	}
	if val != "world" {
		t.Fatalf("expected 'world', got %q", val)
	}
	val, ok = e.Get("deleted")
	if ok {
		t.Fatal("expected key 'deleted' to be absent")
	}
}
