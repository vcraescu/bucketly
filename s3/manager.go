package s3

import (
	"context"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
)

type (
	BucketManager struct {
		bucket *Bucket
	}
)

func NewBucketManager(bucket *Bucket) *BucketManager {
	return &BucketManager{bucket: bucket}
}

func (m *BucketManager) Create(ctx context.Context) error {
	_, err := m.bucket.client.CreateBucketWithContext(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(m.bucket.Name()),
	})
	if err != nil {
		if err, ok := err.(awserr.RequestFailure); ok {
			if err.StatusCode() == 409 {
				return nil
			}
		}

		return err
	}

	return m.bucket.client.WaitUntilBucketExists(&s3.HeadBucketInput{
		Bucket: aws.String(m.bucket.Name()),
	})
}

func (m *BucketManager) Remove(ctx context.Context) error {
	_, err := m.bucket.client.DeleteBucketWithContext(ctx, &s3.DeleteBucketInput{
		Bucket: aws.String(m.bucket.Name()),
	})
	if err != nil {
		return err
	}

	return m.bucket.client.WaitUntilBucketNotExists(&s3.HeadBucketInput{
		Bucket: aws.String(m.bucket.Name()),
	})
}

func (m *BucketManager) Clean(ctx context.Context) error {
	return m.bucket.RemoveAll(ctx, "/")
}
