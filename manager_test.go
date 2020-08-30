package bucketly_test

import (
	"context"
	"github.com/stretchr/testify/suite"
	"github.com/vcraescu/bucketly"
	"github.com/vcraescu/bucketly/local"
	"os"
	"testing"
)

type BucketManagerTestSuite struct {
	suite.Suite

	newBucket  func(name string) bucketly.Bucket
	newManager func(bucket bucketly.Bucket) bucketly.BucketManager
}

func TestS3BucketManagerTestSuite(t *testing.T) {
	s := new(BucketManagerTestSuite)
	s.newBucket = newS3Bucket
	s.newManager = newS3BucketManager

	suite.Run(t, s)
}

func TestLocalBucketManagerTestSuite(t *testing.T) {
	s := new(BucketManagerTestSuite)
	s.newBucket = func(name string) bucketly.Bucket {
		return local.NewBucket(name)
	}

	s.newManager = newLocalBucketManager

	suite.Run(t, s)
}

func (suite *BucketManagerTestSuite) TestCreateAndRemove() {
	ctx := context.Background()
	bucket := suite.newBucket(os.Getenv("AWS_S3_BUCKET"))
	manager := suite.newManager(bucket)
	if !suite.NoError(manager.Create(ctx)) {
		return
	}

	suite.NoError(manager.Create(ctx))
	suite.NoError(manager.Remove(ctx))
}

func (suite *BucketManagerTestSuite) TestClean() {
	ctx := context.Background()
	bucket := suite.newBucket(os.Getenv("AWS_S3_BUCKET"))
	manager := suite.newManager(bucket)
	if !suite.NoError(manager.Create(ctx)) {
		return
	}

	suite.NoError(suite.createDeepDir(ctx, bucket, "test_clean/"))
	suite.NoError(manager.Clean(ctx))
	suite.NoError(manager.Remove(ctx))
}

func (suite *BucketManagerTestSuite) createDeepDir(ctx context.Context, bucket bucketly.Bucket, baseDir string) error {
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
