package main

import (
	"io"
	"net/http"
	"sync"
)

type KVServer struct {
	mp   map[string]string
	lock sync.RWMutex
}

// func (c *KVServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
// 	switch r.URL.Path {
// 	case "/increment":
// 		c.lock.Lock()
// 		c.count++
// 		c.lock.Unlock()
// 	case "/counter":
// 		c.lock.Lock()
// 		outString := strconv.FormatUint(uint64(c.count), 10)
// 		c.lock.Unlock()
// 		w.Write([]byte(outString))
// 	}
// }

func (s *KVServer) HandleGet(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("name")
	s.lock.RLock()
	val, exists := s.mp[key]
	s.lock.RUnlock()
	if !exists {
		http.Error(w, "Key not found", http.StatusNotFound)
		return
	}
	w.Write([]byte(val))
}

func (s *KVServer) HandlePut(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("name")
	b, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Invalid body", http.StatusInternalServerError)
		return
	}
	val := string(b)
	s.lock.Lock()
	s.mp[key] = val
	s.lock.Unlock()
	w.WriteHeader(http.StatusOK)
}

func (s *KVServer) HandleDelete(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("name")
	s.lock.Lock()
	delete(s.mp, key)
	s.lock.Unlock()
	w.WriteHeader(http.StatusOK)
}

func init() {
	s := &KVServer{}
	s.mp = make(map[string]string)
	http.HandleFunc("GET /key/{name}", s.HandleGet)
	http.HandleFunc("PUT /key/{name}", s.HandlePut)
	http.HandleFunc("DELETE /key/{name}", s.HandleDelete)

}

func main() {
	http.ListenAndServe(":8080", nil)
}
