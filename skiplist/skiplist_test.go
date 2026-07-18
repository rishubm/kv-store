package skiplist
import (
	"testing"
)

func TestSkiplistOperations(t *testing.T) {
	sl := NewSkipList(5)

	// Test Get on empty list
	if _, ok := sl.Get("a"); ok {
		t.Error("expected ok to be false for non-existent key")
	}

	// Test Put and Get
	sl.Put("a", "1")
	sl.Put("b", "2")
	if v, ok := sl.Get("a"); !ok || v != "1" {
		t.Errorf("expected Get(a) to return 1, true; got %s, %t", v, ok)
	}

	// Test Delete
	if !sl.Delete("a") {
		t.Error("expected Delete(a) to return true")
	}
	if _, ok := sl.Get("a"); ok {
		t.Error("expected key to be deleted")
	}

	// Test Iterator
	it := sl.Iterator()
	if !it.Next() {
		t.Error("expected next to exist")
	}
	if it.Key() != "b" || it.Value() != "2" {
		t.Errorf("expected iterator to point to ('b', '2'), got ('%s', '%s')", it.Key(), it.Value())
	}
}