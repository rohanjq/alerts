package store

import (
	"context"
	"testing"
	"time"
)

func TestMemoryPartitionLeaseExcludesAnotherOwner(t *testing.T) {
	repository := NewMemory()
	partitions := []int{1, 3, 5}
	if ok, err := repository.AcquirePartitions(context.Background(), "one", partitions, time.Second); err != nil || !ok {
		t.Fatalf("first acquire=%v err=%v", ok, err)
	}
	if ok, err := repository.AcquirePartitions(context.Background(), "two", partitions, time.Second); err != nil || ok {
		t.Fatalf("conflicting acquire=%v err=%v", ok, err)
	}
	if err := repository.ReleasePartitions(context.Background(), "one"); err != nil {
		t.Fatal(err)
	}
	if ok, err := repository.AcquirePartitions(context.Background(), "two", partitions, time.Second); err != nil || !ok {
		t.Fatalf("acquire after release=%v err=%v", ok, err)
	}
}
