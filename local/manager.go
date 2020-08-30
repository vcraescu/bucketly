package local

import (
	"context"
	"os"
)

type (
	BucketManager struct {
		bucket *Bucket
	}
)

func NewBucketManager(bucket *Bucket) *BucketManager {
	return &BucketManager{bucket: bucket}
}

func (m *BucketManager) Create(_ context.Context) error {
	return os.MkdirAll(m.bucket.Name(), 0744)
}

func (m *BucketManager) Remove(_ context.Context) error {
	return os.RemoveAll(m.bucket.Name())
}

func (m *BucketManager) Clean(_ context.Context) error {
	return os.RemoveAll(m.bucket.Name())
}
