package local

import (
	"context"
	"fmt"
	"github.com/vcraescu/bucketly"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
)

const (
	defaultDirMode  os.FileMode = 0744
	defaultFileMode os.FileMode = 0666
)

type (
	Bucket struct {
		name string
	}

	listIterator struct {
		name   string
		bucket *Bucket
		queue  []os.FileInfo
	}
)

func NewBucket(name string) *Bucket {
	return &Bucket{name: name}
}

func (b *Bucket) PathSeparator() rune {
	return os.PathSeparator
}

func (b *Bucket) Name() string {
	return b.name
}

func (b *Bucket) Read(ctx context.Context, name string) ([]byte, error) {
	r, err := b.NewReader(ctx, name)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	return ioutil.ReadAll(r)
}

func (b *Bucket) NewReader(ctx context.Context, name string) (io.ReadCloser, error) {
	s, err := b.Stat(ctx, name)
	if err != nil {
		return nil, err
	}

	if s.IsDir() {
		return nil, fmt.Errorf("%s is a directory", name)
	}

	return os.Open(b.realPath(name))
}

func (b *Bucket) Write(ctx context.Context, name string, data []byte, opts ...bucketly.WriteOption) (int, error) {
	dir := filepath.Dir(name)
	if err := b.MkdirAll(ctx, dir); err != nil {
		return 0, err
	}

	w, err := b.NewWriter(ctx, name, opts...)
	if err != nil {
		return 0, err
	}
	defer w.Close()

	return w.Write(data)
}

func (b *Bucket) NewWriter(_ context.Context, name string, opts ...bucketly.WriteOption) (io.WriteCloser, error) {
	wo := &bucketly.WriteOptions{
		Mode: defaultFileMode,
	}
	for _, opt := range opts {
		opt(wo)
	}

	return os.OpenFile(b.realPath(name), os.O_CREATE|os.O_WRONLY, wo.Mode)
}

func (b *Bucket) Exists(ctx context.Context, name string) (bool, error) {
	_, err := b.Stat(ctx, name)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}

		return false, err
	}

	return true, nil
}

func (b *Bucket) Remove(_ context.Context, name string) error {
	return os.Remove(b.realPath(name))
}

func (b *Bucket) Stat(_ context.Context, name string) (bucketly.Item, error) {
	fi, err := os.Stat(b.realPath(name))
	if err != nil {
		return nil, err
	}

	return b.fileInfoToItem(name, fi), nil
}

func (b *Bucket) Mkdir(ctx context.Context, name string, opts ...bucketly.WriteOption) error {
	wo := &bucketly.WriteOptions{
		Mode: defaultDirMode,
	}
	for _, opt := range opts {
		opt(wo)
	}

	item, err := b.Stat(ctx, name)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil
		}
	}

	if item != nil && item.IsDir() {
		return b.Chmod(ctx, name, wo.Mode)
	}

	if err := os.Mkdir(b.realPath(name), wo.Mode); err != nil {
		if err != os.ErrExist {
			return err
		}
	}

	return nil
}

func (b *Bucket) MkdirAll(_ context.Context, name string, opts ...bucketly.WriteOption) error {
	wo := &bucketly.WriteOptions{
		Mode: defaultDirMode,
	}
	for _, opt := range opts {
		opt(wo)
	}

	return os.MkdirAll(b.realPath(name), wo.Mode)
}

func (b *Bucket) Chmod(_ context.Context, name string, mode os.FileMode) error {
	return os.Chmod(b.realPath(name), mode)
}

func (b *Bucket) RemoveAll(_ context.Context, name string) error {
	return os.RemoveAll(b.realPath(name))
}

func (b *Bucket) Rename(_ context.Context, from string, to string, opts ...bucketly.CopyOption) error {
	return os.Rename(b.realPath(from), b.realPath(to))
}

func (b *Bucket) Copy(ctx context.Context, from bucketly.Item, to string, opts ...bucketly.CopyOption) error {
	if from.IsDir() {
		return b.MkdirAll(ctx, to)
	}

	if err := b.MkdirAll(ctx, bucketly.Dir(b, to)); err != nil {
		return err
	}

	src, err := from.Open(ctx)
	if err != nil {
		return nil
	}
	defer src.Close()

	co := &bucketly.CopyOptions{
		Mode: from.Mode(),
	}
	for _, opt := range opts {
		opt(co)
	}

	if co.Mode == 0 {
		co.Mode = defaultFileMode
	}

	dest, err := os.OpenFile(b.realPath(to), os.O_RDWR|os.O_CREATE, co.Mode)
	if err != nil {
		return err
	}
	defer dest.Close()

	if deadline, ok := ctx.Deadline(); ok {
		dest.SetDeadline(deadline)
	}

	_, err = io.Copy(dest, src)
	if err != nil {
		return err
	}

	return nil
}

func (b *Bucket) CopyAll(ctx context.Context, from bucketly.Item, to string, opts ...bucketly.CopyOption) error {
	if err := b.MkdirAll(ctx, bucketly.Dir(b, to)); err != nil {
		return err
	}

	toItem := bucketly.NewItem(b, to)
	toItem.SetMode(from.Mode())

	return bucketly.CopyAll(ctx, from, toItem, opts...)
}

func (b *Bucket) Copy2(ctx context.Context, from string, to string, opts ...bucketly.CopyOption) error {
	fromItem, err := b.Stat(ctx, from)
	if err != nil {
		return err
	}

	return b.Copy(ctx, fromItem, to, opts...)
}

func (b *Bucket) CopyAll2(ctx context.Context, from string, to string, opts ...bucketly.CopyOption) error {
	fromItem, err := b.Stat(ctx, from)
	if err != nil {
		return err
	}

	return b.CopyAll(ctx, fromItem, to, opts...)
}

func (b *Bucket) Walk(_ context.Context, dir string, walkFunc bucketly.WalkFunc) error {
	dir = bucketly.Clean(b, dir)
	err := filepath.Walk(b.realPath(dir), func(path string, info os.FileInfo, err error) error {
		name, err := filepath.Rel(b.name, path)
		if name == dir {
			return nil
		}

		if err != nil {
			return err
		}

		item := b.fileInfoToItem(name, info)

		if err := walkFunc(item, err); err != nil {
			if err == bucketly.ErrSkipWalkDir {
				return filepath.SkipDir
			}

			return err
		}

		return nil
	})

	if err != bucketly.ErrStopWalk {
		return err
	}

	return nil
}

func (b *Bucket) Items(name string) (bucketly.ListIterator, error) {
	name = bucketly.Clean(b, name)
	iter := &listIterator{
		name:   name,
		bucket: b,
	}

	return iter, nil
}

func (b *Bucket) realPath(name string) string {
	name, err := bucketly.Sanitize(b, name)
	if err != nil {
		return ""
	}

	return bucketly.Join(b, b.name, name)
}

func (b *Bucket) fileInfoToItem(name string, info os.FileInfo) bucketly.Item {
	item := bucketly.NewItem(b, name)
	item.SetMode(info.Mode())
	item.SetModeTime(info.ModTime())
	item.SetDir(info.IsDir())
	item.SetSize(info.Size())
	item.SetSys(info.Sys())

	return item
}

func (i *listIterator) Next(ctx context.Context) (bucketly.Item, error) {
	if i.queue == nil {
		item, err := i.bucket.Stat(ctx, i.name)
		if err != nil {
			return nil, err
		}

		if !item.IsDir() {
			i.queue = make([]os.FileInfo, 0)

			return item, nil
		}

		infos, err := ioutil.ReadDir(i.bucket.realPath(item.Name()))
		if err != nil {
			return nil, err
		}

		i.queue = infos
	}

	if len(i.queue) == 0 {
		return nil, io.EOF
	}

	info := i.queue[0]
	i.queue = i.queue[1:]
	name := strings.TrimLeft(bucketly.Join(i.bucket, i.name, info.Name()), string(i.bucket.PathSeparator()))

	return i.bucket.fileInfoToItem(name, info), nil
}

func (i *listIterator) Close() error {
	i.queue = nil

	return nil
}
