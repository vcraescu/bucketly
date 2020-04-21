package s3

import (
	"context"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"strings"
)

func directorize(s string) string {
	if strings.HasSuffix(s, string(pathSeparator)) {
		return s
	}

	return s + string(pathSeparator)
}

func waitUntilKeyExists(ctx context.Context, svc *s3.S3, bucket, key string) error {
	return svc.WaitUntilObjectExistsWithContext(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
}

func waitUntilKeyNotExists(ctx context.Context, svc *s3.S3, bucket, key string) error {
	return svc.WaitUntilObjectNotExistsWithContext(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
}

func isNotExists(err error) bool {
	if err1, ok := err.(awserr.RequestFailure); ok && err1.StatusCode() == 404 {
		return true
	}

	return false
}
