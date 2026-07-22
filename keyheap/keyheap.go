package keyheap

type HeapNode struct {
	Key       string
	Value     string
	FileIndex int // which file this node came from
}

type KeyHeap []HeapNode

func (k KeyHeap) Len() int {
	return len(k)
}

func (k KeyHeap) Less(i, j int) bool {
	if k[i].Key < k[j].Key {
		return true
	}
	// if keys are equal, newer file wins
	if k[i].Key == k[j].Key {
		return k[i].FileIndex > k[j].FileIndex
	}
	return false
}

func (k KeyHeap) Swap(i, j int) {
	k[i], k[j] = k[j], k[i]
}

func (k *KeyHeap) Push(x any) {
	*k = append(*k, x.(HeapNode))
}

func (k *KeyHeap) Pop() any {
	old := *k
	n := len(old)
	x := old[n-1]
	*k = old[0 : n-1]
	return x
}

func NewKeyHeap() KeyHeap {
	return make([]HeapNode, 0)
}
