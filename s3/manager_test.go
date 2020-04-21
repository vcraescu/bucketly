package s3_test

import (
	"context"
	"github.com/stretchr/testify/suite"
	"github.com/vcraescu/bucketly"
	"github.com/vcraescu/bucketly/s3"
	"os"
	"testing"
)

type S3BucketManagerTestSuite struct {
	suite.Suite
}

func (suite *S3BucketManagerTestSuite) TestCreateAndRemove() {
	ctx := context.Background()
	bucket := suite.newBucket(os.Getenv("AWS_S3_BUCKET"))
	manager := s3.NewBucketManager(bucket)
	if !suite.NoError(manager.Create(ctx)) {
		return
	}

	suite.NoError(manager.Create(ctx))
	suite.NoError(manager.Remove(ctx))
}

func (suite *S3BucketManagerTestSuite) TestClean() {
	ctx := context.Background()
	bucket := suite.newBucket(os.Getenv("AWS_S3_BUCKET"))
	manager := s3.NewBucketManager(bucket)
	if !suite.NoError(manager.Create(ctx)) {
		return
	}

	suite.NoError(suite.createDeepDir(ctx, bucket, "test_clean/"))
	suite.NoError(manager.Clean(ctx))
	suite.NoError(manager.Remove(ctx))
}

func (suite *S3BucketManagerTestSuite) newBucket(name string) *s3.Bucket {
	bucket, err := s3.NewBucket(
		name,
		s3.WithRegion(os.Getenv("AWS_S3_REGION")),
		s3.WithAccessKey(os.Getenv("AWS_S3_ACCESS_KEY")),
		s3.WithSecretAccessKey(os.Getenv("AWS_S3_SECRET_ACCESS_KEY")),
		s3.WithEndpoint(os.Getenv("AWS_S3_ENDPOINT")),
	)
	if err != nil {
		panic(err)
	}

	return bucket
}

func (suite *S3BucketManagerTestSuite) createDeepDir(ctx context.Context, bucket bucketly.Bucket, baseDir string) error {
	if err := bucket.MkdirAll(ctx, bucketly.Join(bucket, baseDir, "test1/test2/test3/")); err != nil {
		return err
	}

	if err := bucket.MkdirAll(ctx, bucketly.Join(bucket, baseDir, "test1/test3/test4/")); err != nil {
		return err
	}

	if _, err := bucket.Write(ctx, bucketly.Join(bucket, baseDir, "test1/test2/foo2.txt"), []byte("12345")); err != nil {
		return err
	}

	if _, err := bucket.Write(ctx, bucketly.Join(bucket, baseDir, "test1/foo1.txt"), []byte("12345")); err != nil {
		return err
	}
	if _, err := bucket.Write(ctx, bucketly.Join(bucket, baseDir, "test1/foo11.txt"), []byte("12345")); err != nil {
		return err
	}

	if _, err := bucket.Write(ctx, bucketly.Join(bucket, baseDir, "test1/test2/test3/foo3.txt"), []byte("12345")); err != nil {
		return err
	}

	if _, err := bucket.Write(ctx, bucketly.Join(bucket, baseDir, "test1/test2/test3/foo31.txt"), []byte("12345")); err != nil {
		return err
	}

	if _, err := bucket.Write(ctx, bucketly.Join(bucket, baseDir, "test1/test2/test3/foo32.txt"), []byte("12345")); err != nil {
		return err
	}

	return nil
}

func TestS3BucketManagerTestSuite(t *testing.T) {
	suite.Run(t, new(S3BucketManagerTestSuite))
}
