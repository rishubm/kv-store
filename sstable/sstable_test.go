package sstable

import (
	"os"
	"testing"
	"time"

	"github.com/rishubm/kv-store/memtable"
)

func TestMain(m *testing.M) {
	os.MkdirAll("./testdata", 0755)
	code := m.Run()
	os.RemoveAll("./testdata")
	os.Exit(code)
}

// TestMerge verifies that Merge correctly:
//   - Combines keys across multiple SSTable files
//   - Keeps the newest value when the same key appears in multiple files
//   - Drops tombstoned keys from the merged output
func TestMerge(t *testing.T) {
	dir := "./testdata/"
	// SSTable 1 (older): a=1, b=2, c=old, e=alive
	mem1 := memtable.NewMemtable()
	mem1.Put("a", "1")
	mem1.Put("b", "2")
	mem1.Put("c", "old")
	mem1.Put("e", "alive")
	mem1.Put("f", "dead")
	mem1.Delete("f") // make it a tombstone in 1 and bring it back in 2
	path1, err := Write(mem1, dir)
	if err != nil {
		t.Fatalf("Write mem1: %v", err)
	}

	// Ensure a distinct timestamp for the filename ordering to be correct
	time.Sleep(time.Millisecond)

	// SSTable 2 (newer): c=new, d=4, e=TOMBSTONE
	mem2 := memtable.NewMemtable()
	mem2.Put("c", "new")
	mem2.Put("d", "4")
	mem2.Delete("e") // tombstone: e was alive in SSTable 1, now deleted
	mem2.Put("f", "5")
	path2, err := Write(mem2, dir)
	if err != nil {
		t.Fatalf("Write mem2: %v", err)
	}

	mergedPath, err := Merge([]string{path1, path2}, dir)
	if err != nil {
		t.Fatalf("Merge: %v", err)
	}

	cases := []struct {
		key     string
		wantVal string
		wantOk  bool
	}{
		{"a", "1", true},       // only in older SSTable
		{"b", "2", true},       // only in older SSTable
		{"c", "new", true},     // newer SSTable wins over "old"
		{"d", "4", true},       // only in newer SSTable
		{"e", "", false},       // tombstoned in newer SSTable — must not appear
		{"missing", "", false}, // never written
		{"f", "5", true},       // revived in newer SSTable
	}

	for _, tc := range cases {
		val, ok, err := Read(mergedPath, tc.key)
		if err != nil {
			t.Fatalf("Read(%q): %v", tc.key, err)
		}
		if ok != tc.wantOk {
			t.Errorf("key %q: present=%v, want %v", tc.key, ok, tc.wantOk)
		}
		if ok && val != tc.wantVal {
			t.Errorf("key %q: got %q, want %q", tc.key, val, tc.wantVal)
		}
	}
}
