package main

import (
	"net/http"
	"strconv"
	"sync"
)

type CounterServer struct {
	count uint32
	lock  sync.Mutex
}

func (c *CounterServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/increment":
		c.lock.Lock()
		c.count++
		c.lock.Unlock()
	case "/counter":
		c.lock.Lock()
		outString := strconv.FormatUint(uint64(c.count), 10)
		c.lock.Unlock()
		w.Write([]byte(outString))
	}
}

func init() {
	s := &CounterServer{}
	http.Handle("/increment", s)
	http.Handle("/counter", s)
}

func main() {
	http.ListenAndServe(":8080", nil)
}
