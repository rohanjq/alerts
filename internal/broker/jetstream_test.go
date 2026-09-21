package broker

import (
	"testing"
	"time"
)

func TestRetryDelayIsExponentialAndCapped(t *testing.T) {
	tests := []struct {
		delivery uint64
		want     time.Duration
	}{
		{0, time.Second},
		{1, time.Second},
		{2, 2 * time.Second},
		{9, 256 * time.Second},
		{10, 5 * time.Minute},
		{100, 5 * time.Minute},
	}
	for _, test := range tests {
		if got := retryDelay(test.delivery); got != test.want {
			t.Fatalf("retryDelay(%d)=%s want=%s", test.delivery, got, test.want)
		}
	}
}
