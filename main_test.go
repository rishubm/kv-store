package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
)

// TestBasicOperations tests sequential GET, PUT, and DELETE.
func TestBasicOperations(t *testing.T) {
	server := httptest.NewServer(http.DefaultServeMux)
	defer server.Close()

	client := server.Client()

	// 1. GET non-existent key -> should be 404
	resp, err := client.Get(server.URL + "/key/testkey")
	if err != nil {
		t.Fatalf("GET failed: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("expected 404 Not Found for non-existent key, got %d", resp.StatusCode)
	}

	// 2. PUT key=testkey, value=hello
	req, err := http.NewRequest(http.MethodPut, server.URL+"/key/testkey", bytes.NewBufferString("hello"))
	if err != nil {
		t.Fatalf("failed to create PUT request: %v", err)
	}
	resp, err = client.Do(req)
	if err != nil {
		t.Fatalf("PUT failed: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		t.Errorf("expected 200 OK or 201 Created for PUT, got %d", resp.StatusCode)
	}

	// 3. GET key -> should be "hello"
	resp, err = client.Get(server.URL + "/key/testkey")
	if err != nil {
		t.Fatalf("GET failed: %v", err)
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatalf("failed to read body: %v", err)
	}
	if string(body) != "hello" {
		t.Errorf("expected value 'hello', got %q", string(body))
	}

	// 4. DELETE key
	req, err = http.NewRequest(http.MethodDelete, server.URL+"/key/testkey", nil)
	if err != nil {
		t.Fatalf("failed to create DELETE request: %v", err)
	}
	resp, err = client.Do(req)
	if err != nil {
		t.Fatalf("DELETE failed: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200 OK for DELETE, got %d", resp.StatusCode)
	}

	// 5. GET key again -> should be 404
	resp, err = client.Get(server.URL + "/key/testkey")
	if err != nil {
		t.Fatalf("GET failed: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("expected 404 Not Found after deletion, got %d", resp.StatusCode)
	}
}

// TestConcurrentKV hammers the KV store with concurrent reads, writes, and deletes
// to ensure synchronization is correct and Go's race detector is satisfied.
func TestConcurrentKV(t *testing.T) {
	server := httptest.NewServer(http.DefaultServeMux)
	defer server.Close()

	const numGoroutines = 20
	const opsPerGoroutine = 50

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(workerID int) {
			defer wg.Done()
			client := server.Client()

			for j := 0; j < opsPerGoroutine; j++ {
				key := fmt.Sprintf("key-%d-%d", workerID, j)
				val := fmt.Sprintf("val-%d-%d", workerID, j)

				// 1. PUT
				req, err := http.NewRequest(http.MethodPut, server.URL+"/key/"+key, bytes.NewBufferString(val))
				if err == nil {
					if resp, err := client.Do(req); err == nil {
						resp.Body.Close()
					}
				}

				// 2. GET
				targetKey := fmt.Sprintf("key-%d-%d", (workerID+1)%numGoroutines, j)
				resp, err := client.Get(server.URL + "/key/" + targetKey)
				if err == nil {
					resp.Body.Close()
				}

				// 3. DELETE
				req, err = http.NewRequest(http.MethodDelete, server.URL+"/key/"+key, nil)
				if err == nil {
					if resp, err := client.Do(req); err == nil {
						resp.Body.Close()
					}
				}
			}
		}(i)
	}

	wg.Wait()
}

func TestWALPersistence(t *testing.T) {
	// 1. Back up any existing `./wal.log`
	const targetWal = "./wal.log"
	const backupWal = "./wal.log.bak"
	
	backedUp := false
	if _, err := os.Stat(targetWal); err == nil {
		err := os.Rename(targetWal, backupWal)
		if err != nil {
			t.Fatalf("failed to backup wal.log: %v", err)
		}
		backedUp = true
	}
	
	defer func() {
		// Clean up the test wal
		os.Remove(targetWal)
		// Restore backup
		if backedUp {
			os.Rename(backupWal, targetWal)
		}
	}()
	
	// Ensure start with a fresh slate (wal.log removed)
	os.Remove(targetWal)
	
	// 2. Append some logs using AppendLog
	if _, err := AppendLog("PUT", "k1", "v1"); err != nil {
		t.Fatalf("failed to append log: %v", err)
	}
	if _, err := AppendLog("PUT", "k2", "v2"); err != nil {
		t.Fatalf("failed to append log: %v", err)
	}
	if _, err := AppendLog("DELETE", "k1", ""); err != nil {
		t.Fatalf("failed to append log: %v", err)
	}
	if _, err := AppendLog("PUT", "k3", "v3"); err != nil {
		t.Fatalf("failed to append log: %v", err)
	}
	
	// 3. Create a new KVServer and replay the log
	server := &KVServer{
		mp: make(map[string]string),
	}
	
	err := ReplayLog(server)
	if err != nil {
		t.Fatalf("failed to replay log: %v", err)
	}
	
	// 4. Verify replayed state
	expected := map[string]string{
		"k2": "v2",
		"k3": "v3",
	}
	
	if len(server.mp) != len(expected) {
		t.Errorf("expected map length %d, got %d. Map content: %v", len(expected), len(server.mp), server.mp)
	}
	for k, expectedVal := range expected {
		val, exists := server.mp[k]
		if !exists {
			t.Errorf("expected key %q to exist, but it was not found", k)
		} else if val != expectedVal {
			t.Errorf("expected key %q to have value %q, got %q", k, expectedVal, val)
		}
	}
}

