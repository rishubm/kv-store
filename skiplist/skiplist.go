package skiplist

import (
	"math/rand/v2"
	"sync/atomic"
)

type SkipList interface {
	// Put inserts or updates a key-value pair.
	Put(key string, value string)

	// Get retrieves the value associated with the key. Returns (value, true) if found.
	Get(key string) (string, bool)

	// Delete removes the key from the SkipList. Returns true if key was found and deleted.
	Delete(key string) bool

	// Iterator returns an iterator to traverse the SkipList in sorted order.
	Iterator() Iterator

	// Returns the approximate size in bytes of the keys and values.
	ApproximateSize() uint64
}

type Iterator interface {
	// Next moves the iterator to the next element. Returns false if there are no more elements.
	Next() bool

	// Key returns the current element's key.
	Key() string

	// Value returns the current element's value.
	Value() string
}

type Node struct {
	key   string
	value string
	// next holds pointers to the next node at each level.
	// next[0] is the bottom level, next[h] is level h.
	next []*Node
}

type skipListImpl struct {
	head     *Node
	maxLevel int
	level    int     // Current highest non-empty level
	p        float64 // Probability of promoting a node to the next level (typically 0.5 or 0.25)
	size     uint64
}

type iteratorImpl struct {
	curr *Node
}

func NewSkipList(maxLevel int) SkipList {
	head := &Node{"", "", make([]*Node, maxLevel)}
	return &skipListImpl{head, maxLevel, 0, 0.5, 0}
}

func (sl *skipListImpl) Get(key string) (string, bool) {
	lvl := sl.level
	curr := sl.head
	for lvl >= 0 {
		if curr.next[lvl] == nil || curr.next[lvl].key > key {
			// reached the end of a chain or overshot, need more density
			lvl--
		} else if curr.next[lvl].key == key {
			// found it
			return curr.next[lvl].value, true
		} else {
			// keep searching this level
			curr = curr.next[lvl]
		}
	}
	return "", false
}

func (sl *skipListImpl) Put(key string, value string) {
	lvl := sl.level
	curr := sl.head
	// keeps track of the last seen at every level while traversing
	updates := make([]*Node, sl.maxLevel)
	for lvl >= 0 {
		if curr.next[lvl] == nil || curr.next[lvl].key > key {
			// reached the end of a chain or overshot, need more density
			updates[lvl] = curr
			lvl--
		} else if curr.next[lvl].key == key {
			// found it, just update the value
			sl.size = sl.size - uint64(len(curr.next[lvl].value)) + uint64(len(value))
			curr.next[lvl].value = value
			return
		} else {
			// keep searching this level
			curr = curr.next[lvl]
		}
	}
	insert_lvl := sl.genLevel()
	new_node := &Node{key, value, make([]*Node, insert_lvl+1)}

	for i := 0; i <= insert_lvl; i++ {
		// if it is being inserted higher than the current max level, update the head to point to it
		if i > sl.level {
			sl.head.next[i] = new_node
			continue
		}
		// otherwise update each row's previous one
		new_node.next[i] = updates[i].next[i]
		updates[i].next[i] = new_node
	}
	if insert_lvl > sl.level {
		sl.level = insert_lvl
	}
	sl.size += uint64(len(key) + len(value))
}

func (sl *skipListImpl) genLevel() int {
	lvl := 0
	for rand.Float64() < sl.p && lvl < sl.maxLevel-1 {
		lvl++
	}
	return lvl
}

func (sl *skipListImpl) Delete(key string) bool {
	lvl := sl.level
	curr := sl.head
	//  the level we found the key at
	found_lvl := -1
	updates := make([]*Node, sl.maxLevel)
	for lvl >= 0 {
		if curr.next[lvl] == nil || curr.next[lvl].key >= key {
			// found the first node on this level not less than target
			updates[lvl] = curr
			if curr.next[lvl] != nil && curr.next[lvl].key == key && found_lvl == -1 {
				sl.size -= uint64(len(curr.next[lvl].value) + len(key))
				found_lvl = lvl
			}
			lvl--
		} else {
			// keep searching this level
			curr = curr.next[lvl]
		}
	}
	if found_lvl == -1 {
		return false
	}
	// go up to the highest level of the target
	for i := 0; i <= found_lvl; i++ {
		// if updates points to
		if updates[i].next[i].key == key {
			// delete it
			tgt := updates[i].next[i]
			updates[i].next[i] = tgt.next[i]
			tgt.next[i] = nil
		}
	}
	// if we removed from the top level and nows it's empty, remove it
	for sl.level > 0 && sl.head.next[sl.level] == nil {
		sl.level--
	}
	return true
}

func (sl *skipListImpl) ApproximateSize() uint64 {
	return atomic.LoadUint64(&sl.size)
}

func (sl *skipListImpl) Iterator() Iterator {
	return &iteratorImpl{sl.head}
}

func (it *iteratorImpl) Next() bool {
	if it.curr != nil {
		it.curr = it.curr.next[0]
	}
	return it.curr != nil
}

func (it *iteratorImpl) Key() string {
	if it.curr != nil {
		return it.curr.key
	}
	return ""
}

func (it *iteratorImpl) Value() string {
	if it.curr != nil {
		return it.curr.value
	}
	return ""
}
