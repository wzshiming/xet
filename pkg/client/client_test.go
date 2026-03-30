package client

import (
	"testing"
	"time"
)

func TestNewClient(t *testing.T) {
	client := NewClient(ClientOptions{
		BaseURL:   "https://example.com/",
		Token:     "test-token",
		Namespace: "test-ns",
		Timeout:   10 * time.Second,
	})

	if client.baseURL != "https://example.com" {
		t.Errorf("Expected baseURL 'https://example.com', got '%s'", client.baseURL)
	}
	if client.token != "test-token" {
		t.Errorf("Expected token 'test-token', got '%s'", client.token)
	}
	if client.namespace != "test-ns" {
		t.Errorf("Expected namespace 'test-ns', got '%s'", client.namespace)
	}
}
