package mock

import (
	"context"
	"github.com/stretchr/testify/mock"
	"github.com/vcraescu/bucketly"
	"io"
	"os"
)

type BucketMock struct {
	mock.Mock
}

func (b *BucketMock) PathSeparator() rune {
	args := b.Called()

	return args.Get(0).(rune)
}

func (b *BucketMock) Name() string {
	panic("implement me")
}

func (b *BucketMock) Read(ctx context.Context, name string) ([]byte, error) {
	panic("implement me")
}

func (b *BucketMock) NewReader(ctx context.Context, name string) (io.ReadCloser, error) {
	args := b.Called(ctx, name)

	return args.Get(0).(io.ReadCloser), args.Error(1)
}

func (b *BucketMock) Write(ctx context.Context, name string, data []byte, opts ...bucketly.WriteOption) (int, error) {
	panic("implement me")
}

func (b *BucketMock) NewWriter(ctx context.Context, name string, opts ...bucketly.WriteOption) (io.WriteCloser, error) {
	panic("implement me")
}

func (b *BucketMock) Exists(ctx context.Context, name string) (bool, error) {
	panic("implement me")
}

func (b *BucketMock) Remove(ctx context.Context, name string) error {
	panic("implement me")
}

func (b *BucketMock) Stat(ctx context.Context, name string) (bucketly.Item, error) {
	args := b.Called(ctx, name)

	return args.Get(0).(bucketly.Item), args.Error(1)
}

func (b *BucketMock) Mkdir(ctx context.Context, name string, opts ...bucketly.WriteOption) error {
	panic("implement me")
}

func (b *BucketMock) MkdirAll(ctx context.Context, name string, opts ...bucketly.WriteOption) error {
	panic("implement me")
}

func (b *BucketMock) Chmod(ctx context.Context, name string, mode os.FileMode) error {
	panic("implement me")
}

func (b *BucketMock) RemoveAll(ctx context.Context, name string) error {
	panic("implement me")
}

func (b *BucketMock) Rename(ctx context.Context, from string, to string, opts ...bucketly.CopyOption) error {
	panic("implement me")
}

func (b *BucketMock) Copy(ctx context.Context, from bucketly.Item, to string, opts ...bucketly.CopyOption) error {
	panic("implement me")
}

func (b *BucketMock) CopyAll(ctx context.Context, from bucketly.Item, to string, opts ...bucketly.CopyOption) error {
	panic("implement me")
}

func (b *BucketMock) Copy2(ctx context.Context, from string, to string, opts ...bucketly.CopyOption) error {
	panic("implement me")
}

func (b *BucketMock) CopyAll2(ctx context.Context, from string, to string, opts ...bucketly.CopyOption) error {
	panic("implement me")
}
