package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
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
