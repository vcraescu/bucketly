package bucketly_test

import (
	"context"
	"github.com/stretchr/testify/assert"
	mock2 "github.com/stretchr/testify/mock"
	"github.com/vcraescu/bucketly"
	"github.com/vcraescu/bucketly/mock"
	"io/ioutil"
	"strings"
	"testing"
)

func TestBucketItem_ETag(t *testing.T) {
	bucket := &mock.BucketMock{}
	name := "foo/bar"
	expectedItem := bucketly.NewItem(bucket, name)
	expectedItem.SetETag("1234")
	bucket.
		On("Stat", mock2.Anything, name).
		Return(expectedItem, nil).
		Once()

	item := bucketly.NewItem(bucket, name)
	a := assert.New(t)
	expectedEtag, err := expectedItem.ETag()
	if !a.NoError(err) {
		return
	}

	actualEtag, err := item.ETag()
	if !a.NoError(err) {
		return
	}
	a.Equal(expectedEtag, actualEtag)

	actualEtag, err = item.ETag()
	if !a.NoError(err) {
		return
	}
	a.Equal(expectedEtag, actualEtag)
}

func TestBucketItem_ETag2(t *testing.T) {
	bucket := &mock.BucketMock{}
	name := "foo/bar"
	item := bucketly.NewItem(bucket, name)
	item.SetETag("test")
	a := assert.New(t)
	etag, err := item.ETag()
	if !a.NoError(err) {
		return
	}

	a.False(bucket.IsMethodCallable(t, "Stat"))
	a.Equal("test", etag)
}

func TestBucketItem_Metadata(t *testing.T) {
	bucket := &mock.BucketMock{}
	name := "foo/bar"
	expectedItem := bucketly.NewItem(bucket, name)
	expectedItem.AddMetadata("foo", "bar")
	bucket.
		On("Stat", mock2.Anything, name).
		Return(expectedItem, nil).
		Once()

	item := bucketly.NewItem(bucket, name)
	a := assert.New(t)
	expectedMetadata, err := expectedItem.Metadata()
	if !a.NoError(err) {
		return
	}

	actualMetadata, err := item.Metadata()
	if !a.NoError(err) {
		return
	}
	a.Equal(expectedMetadata, actualMetadata)

	actualMetadata, err = item.Metadata()
	if !a.NoError(err) {
		return
	}
	a.Equal(expectedMetadata, actualMetadata)
}

func TestBucketItem_Open(t *testing.T) {
	bucket := &mock.BucketMock{}
	name := "foo/bar"
	ctx := context.Background()
	item := bucketly.NewItem(bucket, name)
	bucket.
		On("NewReader", ctx, name).
		Return(ioutil.NopCloser(strings.NewReader("test")), nil).
		Once()

	a := assert.New(t)

	r, err := item.Open(context.Background())
	if !a.NoError(err) {
		return
	}
	s, err := ioutil.ReadAll(r)
	if !a.NoError(err) {
		return
	}

	a.Equal("test", string(s))
}
