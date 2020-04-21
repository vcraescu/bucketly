package s3_test

import (
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/stretchr/testify/assert"
	"github.com/vcraescu/bucketly/s3"
	"testing"
)

func TestDirectorize(t *testing.T) {
	a := assert.New(t)
	a.Equal("/test1/test2/test3/", s3.Directorize("/test1/test2/test3"))
	a.Equal("/test1/test2/test3/", s3.Directorize("/test1/test2/test3/"))
	a.Equal("test/", s3.Directorize("test"))
	a.Equal("test/", s3.Directorize("test/"))
}

func TestIsNotFoundError(t *testing.T) {
	a := assert.New(t)
	err := awserr.NewRequestFailure(nil, 404, "")
	a.True(s3.IsNotExists(err))

	err = awserr.NewRequestFailure(nil, 200, "")
	a.False(s3.IsNotExists(err))
}
