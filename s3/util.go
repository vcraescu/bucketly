package s3

import (
	"context"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/vcraescu/bucketly"
	"strings"
)

func directorize(s string) string {
	s = strings.TrimSpace(s)
	if s == "." {
		return string(pathSeparator)
	}

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
	err1, ok := err.(awserr.RequestFailure)

	return ok && err1.StatusCode() == 404
}

func isDirPath(name string) bool {
	name = strings.TrimSpace(name)
	if name == "" || name == "." {
		name = string(pathSeparator)
	}

	return strings.HasSuffix(name, string(pathSeparator))
}

func cleanPath(b bucketly.PathSeparable, name string) string {
	name = strings.TrimSpace(name)
	isDir := isDirPath(name)
	name = bucketly.Clean(b, name)
	if name == "." {
		name = ""
	}

	if isDir {
		name = directorize(name)
	}

	return name
}

func sanitzePath(b bucketly.PathSeparable, name string) (string, error) {
	isDir := isDirPath(name)
	name, err := bucketly.Sanitize(b, name)
	if err != nil {
		return "", err
	}

	if isDir && name != "/" {
		name += string(b.PathSeparator())
	}

	return name, nil
}
