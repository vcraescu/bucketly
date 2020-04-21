package bucketly

import (
	"context"
	"io"
	"os"
	"sync"
	"time"
)

type (
	BucketItem struct {
		bucket   Bucket
		name     string
		size     int64
		modTime  time.Time
		dir      bool
		mode     os.FileMode
		etag     string
		metadata Metadata
		statOnce sync.Once
		sys      interface{}
		canStat  bool
	}
)

func NewItem(bucket Bucket, name string) *BucketItem {
	return &BucketItem{
		bucket:  bucket,
		name:    name,
		canStat: true,
	}
}

func (i *BucketItem) ETag() (string, error) {
	if i.etag == "" {
		if err := i.stat(); err != nil {
			return "", err
		}
	}

	return i.etag, nil
}

func (i *BucketItem) SetETag(etag string) {
	i.etag = etag
}

func (i *BucketItem) Metadata() (Metadata, error) {
	if i.metadata == nil {
		if err := i.stat(); err != nil {
			return nil, err
		}
	}

	return i.metadata, nil
}

func (i *BucketItem) SetMetadata(metadata Metadata) {
	i.metadata = metadata
}

func (i *BucketItem) AddMetadata(k, v string) {
	if i.metadata == nil {
		i.metadata = make(Metadata)
	}

	i.metadata[k] = v
}

func (i *BucketItem) Name() string {
	return i.name
}

func (i *BucketItem) SetSize(size int64) {
	i.size = size
}

func (i *BucketItem) Size() int64 {
	return i.size
}

func (i *BucketItem) SetMode(mode os.FileMode) {
	i.mode = mode
}

func (i *BucketItem) Mode() os.FileMode {
	return i.mode
}

func (i *BucketItem) SetModeTime(modTime time.Time) {
	i.modTime = modTime
}

func (i *BucketItem) ModTime() time.Time {
	return i.modTime
}

func (i *BucketItem) IsDir() bool {
	return i.dir
}

func (i *BucketItem) SetDir(dir bool) {
	i.dir = dir
}

func (i *BucketItem) Sys() interface{} {
	if i.sys == nil {
		if err := i.stat(); err != nil {
			panic(err)
		}
	}
	return i.sys
}

func (i *BucketItem) Open(ctx context.Context) (io.ReadCloser, error) {
	return i.bucket.NewReader(ctx, i.name)
}

func (i *BucketItem) Bucket() Bucket {
	return i.bucket
}

func (i *BucketItem) String() string {
	return i.name
}

func (i *BucketItem) stat() (err error) {
	if !i.canStat {
		return nil
	}

	if i.metadata == nil || i.etag == "" {
		i.statOnce.Do(func() {
			var tmp Item
			tmp, err = i.bucket.Stat(context.Background(), i.name)
			if err != nil {
				return
			}

			t := tmp.(*BucketItem)
			t.canStat = false

			i.metadata, err = tmp.Metadata()
			if err != nil {
				return
			}

			i.etag, err = tmp.ETag()
			if err != nil {
				return
			}

			i.modTime = tmp.ModTime()
			i.size = tmp.Size()
			i.dir = tmp.IsDir()
			i.sys = tmp.Sys()
		})
	}

	return err
}
