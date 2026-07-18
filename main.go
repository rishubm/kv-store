package main

import (
	"bufio"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
)

type KVServer struct {
	mp   map[string]string
	lock sync.RWMutex
}

const wal_name = "./wal.log"

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
	_, err = AppendLog("PUT", key, val)
	if err != nil {
		http.Error(w, "Failed to write to log", http.StatusInternalServerError)
		s.lock.Unlock()
		return
	}
	s.mp[key] = val
	s.lock.Unlock()

	w.WriteHeader(http.StatusOK)
}

func (s *KVServer) HandleDelete(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("name")

	s.lock.Lock()
	_, err := AppendLog("DELETE", key, "")
	if err != nil {
		http.Error(w, "Failed to write to log", http.StatusInternalServerError)
		s.lock.Unlock()
		return
	}
	delete(s.mp, key)
	s.lock.Unlock()

	w.WriteHeader(http.StatusOK)
}

func AppendLog(op string, k string, v string) (int, error) {
	file, err := os.OpenFile(wal_name, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		return 0, err
	}
	defer file.Close()
	n, err := fmt.Fprintf(file, "%s %s %s\n", op, k, v)
	if err != nil {
		return 0, err
	}
	//imediate durability, extra overhead for blocking call
	err = file.Sync()
	if err != nil {
		return 0, err
	}

	return n, nil
}

func ReplayLog(s *KVServer) error {
	if _, err := os.Stat(wal_name); os.IsNotExist(err) {
		// log doesn't exist, first run not an error
		return nil
	}
	file, err := os.Open(wal_name)
	if err != nil {
		return err
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		split := strings.Split(line, " ")
		op, k, v := split[0], split[1], split[2]
		switch op {
		case "PUT":
			s.mp[k] = v
		case "DELETE":
			delete(s.mp, k)
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return nil
}

func init() {
	s := &KVServer{}
	s.mp = make(map[string]string)
	if err := ReplayLog(s); err != nil {
		return
	}
	http.HandleFunc("GET /key/{name}", s.HandleGet)
	http.HandleFunc("PUT /key/{name}", s.HandlePut)
	http.HandleFunc("DELETE /key/{name}", s.HandleDelete)

}

func main() {
	http.ListenAndServe(":8080", nil)
}
