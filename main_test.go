package main

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// TestConcurrentCounter tests that the counter server is safe from data races
// and correctly tallies concurrent requests.
func TestConcurrentCounter(t *testing.T) {
	// We reset or ensure handlers are registered.
	// Since the user will write main.go, we expect them to register handlers
	// on http.DefaultServeMux or we can call their registration function.
	// To make it easy, we assume they register on http.DefaultServeMux,
	// or we can test their handlers if they use http.DefaultServeMux.
	
	server := httptest.NewServer(http.DefaultServeMux)
	defer server.Close()

	const numGoroutines = 10
	const incrementsPerGoroutine = 100
	const expectedTotal = numGoroutines * incrementsPerGoroutine

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// Launch multiple goroutines to increment the counter concurrently
	for range numGoroutines {
		go func() {
			defer wg.Done()
			client := server.Client()
			for range incrementsPerGoroutine {
				resp, err := client.Post(server.URL+"/increment", "text/plain", nil)
				if err != nil {
					t.Errorf("POST /increment failed: %v", err)
					return
				}
				resp.Body.Close()
				if resp.StatusCode != http.StatusOK {
					t.Errorf("expected status OK; got %v", resp.Status)
					return
				}
			}
		}()
	}

	wg.Wait()

	// Now check the final count
	client := server.Client()
	resp, err := client.Get(server.URL + "/counter")
	if err != nil {
		t.Fatalf("GET /counter failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status OK; got %v", resp.Status)
	}

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read body: %v", err)
	}

	bodyStr := strings.TrimSpace(string(bodyBytes))
	count, err := strconv.Atoi(bodyStr)
	if err != nil {
		t.Fatalf("failed to parse counter value %q: %v", bodyStr, err)
	}

	if count != expectedTotal {
		t.Errorf("expected final count to be %d, but got %d", expectedTotal, count)
	} else {
		fmt.Printf("Success! Counter correctly reached %d under concurrent load.\n", count)
	}
}
