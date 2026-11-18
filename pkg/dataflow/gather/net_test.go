package gather

import "testing"

func TestIsLoopback(t *testing.T) {
	if !isLoopback([]string{"up", "loopback"}) {
		t.Fatal("expected loopback interface")
	}
	if isLoopback([]string{"up", "broadcast"}) {
		t.Fatal("expected non-loopback interface")
	}
}
