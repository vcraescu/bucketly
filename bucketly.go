package bucketly

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

var (
	SkipWalkDir = errors.New("skip walk dir")
)

type (
	WalkFunc func(item Item, err error) error

	WriteOptions struct {
		Metadata   Metadata
		BufferSize int
		Mode       os.FileMode
	}

	WriteOption func(o *WriteOptions)

	CopyOptions struct {
		Metadata Metadata
	}

	CopyOption func(o *CopyOptions)

	CopyFn func(ctx context.Context, from Item, to string) error

	Item interface {
		fmt.Stringer
		os.FileInfo

		Bucket() Bucket
		Open(context.Context) (io.ReadCloser, error)
		ETag() (string, error)
		Metadata() (Metadata, error)
	}

	Walkable interface {
		Walk(ctx context.Context, dir string, walkFunc WalkFunc) error
	}

	PathSeparable interface {
		PathSeparator() rune
	}

	Metadata map[string]string

	Listable interface {
		Items(name string) (ListIterator, error)
	}

	ListIterator interface {
		io.Closer

		Next(ctx context.Context) (Item, error)
	}

	Bucket interface {
		PathSeparable

		Name() string
		Read(ctx context.Context, name string) ([]byte, error)
		NewReader(ctx context.Context, name string) (io.ReadCloser, error)
		Write(ctx context.Context, name string, data []byte, opts ...WriteOption) (int, error)
		NewWriter(ctx context.Context, name string, opts ...WriteOption) (io.WriteCloser, error)
		Exists(ctx context.Context, name string) (bool, error)
		Remove(ctx context.Context, name string) error
		Stat(ctx context.Context, name string) (Item, error)
		Mkdir(ctx context.Context, name string, opts ...WriteOption) error
		MkdirAll(ctx context.Context, name string, opts ...WriteOption) error
		Chmod(ctx context.Context, name string, mode os.FileMode) error
		RemoveAll(ctx context.Context, name string) error
		Rename(ctx context.Context, from string, to string, opts ...CopyOption) error
		Copy(ctx context.Context, from Item, to string, opts ...CopyOption) error
		CopyAll(ctx context.Context, from Item, to string, opts ...CopyOption) error
		Copy2(ctx context.Context, from string, to string, opts ...CopyOption) error
		CopyAll2(ctx context.Context, from string, to string, opts ...CopyOption) error
	}

	BucketManager interface {
		Create(context.Context) error
		Remove(context.Context) error
		Clean(context.Context) error
	}
)

func WithWriteMetadata(metadata Metadata) WriteOption {
	return func(c *WriteOptions) {
		c.Metadata = metadata
	}
}

func WithCopyMetadata(metadata Metadata) CopyOption {
	return func(c *CopyOptions) {
		c.Metadata = metadata
	}
}

func WithWriteBufferSize(bufferSize int) WriteOption {
	return func(c *WriteOptions) {
		c.BufferSize = bufferSize
	}
}

func WithWriteMode(mode os.FileMode) WriteOption {
	return func(c *WriteOptions) {
		c.Mode = mode
	}
}

func Base(b PathSeparable, name string) string {
	if b.PathSeparator() == os.PathSeparator {
		return filepath.Base(name)
	}

	ps := string(b.PathSeparator())
	name = strings.ReplaceAll(name, ps, string(os.PathSeparator))
	name = filepath.Base(name)

	name = strings.ReplaceAll(name, string(os.PathSeparator), ps)

	return name
}

func Clean(b PathSeparable, name string) string {
	if b.PathSeparator() == os.PathSeparator {
		return filepath.Clean(name)
	}

	ps := string(b.PathSeparator())
	name = strings.ReplaceAll(name, ps, string(os.PathSeparator))
	name = filepath.Clean(name)

	name = strings.ReplaceAll(name, string(os.PathSeparator), ps)

	return name
}

func Join(b PathSeparable, elem ...string) string {
	if b.PathSeparator() == os.PathSeparator {
		return filepath.Join(elem...)
	}

	ps := string(b.PathSeparator())
	name := filepath.Join(elem...)
	name = strings.ReplaceAll(name, string(os.PathSeparator), ps)

	return name
}

func CopyAll(ctx context.Context, from Item, to Item, opts ...CopyOption) error {
	bucket := to.Bucket()
	cp := bucket.Copy
	if err := cp(ctx, from, to.Name(), opts...); err != nil {
		return err
	}

	w, ok := from.Bucket().(Walkable)
	if !ok {
		return fmt.Errorf("source bucket is not walkable")
	}

	var wg sync.WaitGroup
	errs := make(chan error)
	wgDone := make(chan struct{})
	err := w.Walk(ctx, from.Name(), func(item Item, err error) error {
		dest := to.Name() + strings.TrimPrefix(item.Name(), from.Name())

		go func() {
			wg.Add(1)
			defer wg.Done()
			if err := cp(ctx, item, dest, opts...); err != nil {
				errs <- err
			}
		}()

		return nil
	})
	if err != nil {
		return err
	}

	go func() {
		wg.Wait()
		close(wgDone)
	}()

	select {
	case <-wgDone:
		return nil
	case err := <-errs:
		return err
	}
}
