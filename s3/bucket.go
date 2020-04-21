package s3

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/vcraescu/bucketly"
	"gocloud.dev/blob"
	"gocloud.dev/blob/s3blob"
	"io"
	"io/ioutil"
	"os"
	"strings"
)

const pathSeparator rune = '/'

type (
	Bucket struct {
		name    string
		config  Config
		session *session.Session
		client  *s3.S3
	}

	Config struct {
		provider        credentials.Provider
		region          string
		endpoint        string
		maxRetries      int
		sess            *session.Session
		accessKeyID     string
		secretAccessKey string
		sessionToken    string
	}

	Option func(cfg *Config)

	listIterator struct {
		name       string
		bucket     *Bucket
		iter       *blob.ListIterator
		blobBucket *blob.Bucket
		queue      []*blob.ListObject
	}

	proxyWriteCloser struct {
		io.WriteCloser
		closed  bool
		OnClose func() func() error
	}

	proxyReadCloser struct {
		io.ReadCloser
		closed  bool
		OnClose func() func() error
	}
)

func WithRegion(region string) Option {
	return func(cfg *Config) {
		cfg.region = region
	}
}

func WithAccessKey(accessKeyID string) Option {
	return func(cfg *Config) {
		cfg.accessKeyID = accessKeyID
	}
}

func WithSecretAccessKey(secretAccessKey string) Option {
	return func(cfg *Config) {
		cfg.secretAccessKey = secretAccessKey
	}
}

func WithSessionToken(sessionToken string) Option {
	return func(cfg *Config) {
		cfg.sessionToken = sessionToken
	}
}

func WithSession(sess *session.Session) Option {
	return func(cfg *Config) {
		cfg.sess = sess
	}
}

func WithMaxRetry(maxRetries int) Option {
	return func(cfg *Config) {
		cfg.maxRetries = maxRetries
	}
}

func WithEndpoint(endpoint string) Option {
	return func(cfg *Config) {
		cfg.endpoint = endpoint
	}
}

func NewBucket(name string, opts ...Option) (*Bucket, error) {
	cfg := Config{}
	for _, opt := range opts {
		opt(&cfg)
	}

	var f *Bucket
	var err error

	if cfg.sess != nil {
		f, err = newWithSession(cfg)
	} else if cfg.provider != nil {
		f, err = newWithProvider(cfg)
	} else if cfg.accessKeyID != "" && cfg.secretAccessKey != "" {
		f, err = newWithStaticCredentials(cfg)
	}

	if err != nil {
		panic(err)
	}
	if f == nil {
		panic("bucket cannot be created because session or credentials provider is missing")
	}

	f.name = name
	f.client = s3.New(f.session)

	return f, nil
}

func newWithStaticCredentials(cfg Config) (*Bucket, error) {
	cfg.provider = &credentials.StaticProvider{Value: credentials.Value{
		AccessKeyID:     cfg.accessKeyID,
		SecretAccessKey: cfg.secretAccessKey,
		SessionToken:    cfg.sessionToken,
	}}

	return newWithProvider(cfg)
}

func newWithSession(cfg Config) (*Bucket, error) {
	f := Bucket{
		config: cfg,
	}

	f.session = cfg.sess

	return &f, nil
}

func newWithProvider(cfg Config) (*Bucket, error) {
	f := Bucket{
		config: cfg,
	}

	awsConfig := aws.Config{
		Credentials: credentials.NewCredentials(cfg.provider),
	}

	if cfg.region != "" {
		awsConfig.Region = aws.String(cfg.region)
	}

	if cfg.maxRetries == 0 {
		awsConfig.MaxRetries = aws.Int(cfg.maxRetries)
	}

	if cfg.endpoint != "" {
		awsConfig.Endpoint = aws.String(cfg.endpoint)
	}

	sess, err := session.NewSessionWithOptions(
		session.Options{
			Config: awsConfig,
		},
	)
	if err != nil {
		return nil, err
	}

	f.session = sess

	return &f, nil
}

func (b *Bucket) Name() string {
	return b.name
}

func (b *Bucket) PathSeparator() rune {
	return pathSeparator
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

func (b *Bucket) Write(ctx context.Context, name string, data []byte, opts ...bucketly.WriteOption) (int, error) {
	w, err := b.NewWriter(ctx, name, opts...)
	if err != nil {
		return 0, err
	}
	defer w.Close()

	return w.Write(data)
}

func (b *Bucket) Read(ctx context.Context, name string) ([]byte, error) {
	r, err := b.NewReader(ctx, name)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	return ioutil.ReadAll(r)
}

func (b *Bucket) Stat(ctx context.Context, name string) (bucketly.Item, error) {
	out, err := b.client.GetObjectWithContext(ctx, &s3.GetObjectInput{
		Bucket: &b.name,
		Key:    &name,
	})
	if err != nil {
		if isNotExists(err) {
			return nil, os.ErrNotExist
		}

		return nil, err
	}

	item := bucketly.NewItem(b, name)
	item.SetDir(strings.HasSuffix(name, string(b.PathSeparator())))
	if out.ETag != nil {
		item.SetETag(*out.ETag)
	}

	if out.ContentLength != nil {
		item.SetSize(*out.ContentLength)
	}

	if out.LastModified != nil {
		item.SetModeTime(*out.LastModified)
	}

	for k, v := range out.Metadata {
		if v != nil {
			item.AddMetadata(k, *v)
		}
	}

	return item, nil
}

func (b *Bucket) Items(name string) (bucketly.ListIterator, error) {
	iter := &listIterator{
		name:   name,
		bucket: b,
	}

	return iter, nil
}

func (b *Bucket) Walk(ctx context.Context, name string, walkFunc bucketly.WalkFunc) error {
	var skipped []string
	isSkipped := func(name string) bool {
		for _, s := range skipped {
			if strings.HasPrefix(name, s) {
				return true
			}
		}

		return false
	}

	iter, err := b.Items(name)
	if err != nil {
		return err
	}
	defer iter.Close()

	for {
		item, err := iter.Next(ctx)
		if err == io.EOF {
			return nil
		}

		if isSkipped(item.Name()) {
			continue
		}

		if err := walkFunc(item, err); err != nil {
			if err == bucketly.SkipWalkDir {
				skipped = append(skipped, item.Name())
				continue
			}

			return err
		}

		if !item.IsDir() {
			continue
		}

		if err := b.Walk(ctx, item.Name(), walkFunc); err != nil {
			return err
		}
	}
}

func (b *Bucket) NewReader(ctx context.Context, name string) (io.ReadCloser, error) {
	bucket, err := b.openBucket(ctx)
	if err != nil {
		return nil, err
	}

	w, err := b.createReader(ctx, bucket, name)

	if err != nil {
		bucket.Close()
		return nil, err
	}

	return &proxyReadCloser{
		ReadCloser: w,
		OnClose: func() func() error {
			return func() error {
				return bucket.Close()
			}
		},
	}, nil
}

func (b *Bucket) NewWriter(ctx context.Context, name string, opts ...bucketly.WriteOption) (io.WriteCloser, error) {
	cfg := &bucketly.WriteOptions{}
	for _, opt := range opts {
		opt(cfg)
	}

	bucket, err := b.openBucket(ctx)
	if err != nil {
		return nil, err
	}

	wo := &blob.WriterOptions{
		Metadata:   cfg.Metadata,
		BufferSize: cfg.BufferSize,
	}
	w, err := b.createWriter(ctx, bucket, name, wo)

	if err != nil {
		bucket.Close()
		return nil, err
	}

	return &proxyWriteCloser{
		WriteCloser: w,
		OnClose: func() func() error {
			return func() error {
				return bucket.Close()
			}
		},
	}, nil
}

func (b *Bucket) Mkdir(ctx context.Context, name string, opts ...bucketly.WriteOption) error {
	name = directorize(bucketly.Clean(b, name))
	_, err := b.Write(ctx, name, []byte{}, opts...)

	return err
}

func (b *Bucket) MkdirAll(ctx context.Context, name string, opts ...bucketly.WriteOption) error {
	name = strings.Trim(bucketly.Clean(b, name), string(b.PathSeparator()))
	var current []string
	tokens := strings.Split(name, string(b.PathSeparator()))
	for _, token := range tokens {
		current = append(current, token)
		if err := b.Mkdir(ctx, bucketly.Join(b, current...), opts...); err != nil {
			return err
		}
	}

	return nil
}

func (b *Bucket) Chmod(ctx context.Context, name string, mode os.FileMode) error {
	panic("not supported")
}

func (b *Bucket) Remove(ctx context.Context, name string) error {
	_, err := b.client.DeleteObjectWithContext(ctx, &s3.DeleteObjectInput{
		Bucket: &b.name,
		Key:    &name,
	})
	if err != nil {
		return err
	}

	return b.WaitUntilNotExists(ctx, name)
}

func (b *Bucket) RemoveAll(ctx context.Context, name string) error {
	list := make([]*s3.ObjectIdentifier, 0)
	err := b.Walk(ctx, name, func(item bucketly.Item, err error) error {
		list = append(list, &s3.ObjectIdentifier{Key: aws.String(item.Name())})
		return nil
	})
	if err != nil {
		return err
	}

	list = append(list, &s3.ObjectIdentifier{Key: &name})
	_, err = b.client.DeleteObjects(&s3.DeleteObjectsInput{
		Bucket: &b.name,
		Delete: &s3.Delete{
			Objects: list,
		},
	})
	if err != nil {
		return err
	}

	if name == string(b.PathSeparator()) {
		return nil
	}

	return b.WaitUntilNotExists(ctx, name)
}

func (b *Bucket) Copy2(ctx context.Context, from string, to string, opts ...bucketly.CopyOption) error {
	item := bucketly.NewItem(b, from)

	return b.Copy(ctx, item, to, opts...)
}

func (b *Bucket) CopyAll2(ctx context.Context, from string, to string, opts ...bucketly.CopyOption) error {
	item := bucketly.NewItem(b, from)

	return b.CopyAll(ctx, item, to, opts...)
}

func (b *Bucket) Rename(ctx context.Context, from string, to string, opts ...bucketly.CopyOption) error {
	if err := b.CopyAll2(ctx, from, to, opts...); err != nil {
		return err
	}

	return b.RemoveAll(ctx, from)
}

func (b *Bucket) Copy(ctx context.Context, from bucketly.Item, to string, opts ...bucketly.CopyOption) error {
	cfg := &bucketly.CopyOptions{}
	for _, opt := range opts {
		opt(cfg)
	}

	src := bucketly.Join(b, from.Bucket().Name(), from.Name())
	if strings.HasSuffix(from.Name(), string(b.PathSeparator())) {
		src += string(b.PathSeparator())
	}

	input := &s3.CopyObjectInput{
		Bucket:     aws.String(b.name),
		CopySource: aws.String(src),
		Key:        aws.String(to),
		Metadata:   aws.StringMap(cfg.Metadata),
	}

	_, err := b.client.CopyObjectWithContext(ctx, input)
	if err != nil {
		return err
	}

	if strings.HasSuffix(to, string(b.PathSeparator())) {
		return nil
	}

	return b.WaitUntilExists(ctx, to)
}

func (b *Bucket) CopyAll(ctx context.Context, from bucketly.Item, to string, opts ...bucketly.CopyOption) error {
	return bucketly.CopyAll(ctx, from, bucketly.NewItem(b, to), opts...)
}

func (b *Bucket) WaitUntilExists(ctx context.Context, name string) error {
	return waitUntilKeyExists(ctx, b.client, b.name, name)
}

func (b *Bucket) WaitUntilNotExists(ctx context.Context, name string) error {
	return waitUntilKeyNotExists(ctx, b.client, b.name, name)
}

func (b *Bucket) openBucket(ctx context.Context) (*blob.Bucket, error) {
	bucket, err := s3blob.OpenBucket(ctx, b.session, b.name, nil)
	if err != nil {
		return nil, fmt.Errorf(`error opening bucket "%s": %w`, b.name, err)
	}

	return bucket, nil
}

func (b *Bucket) createWriter(
	ctx context.Context,
	bucket *blob.Bucket,
	filename string,
	opts *blob.WriterOptions,
) (*blob.Writer, error) {
	w, err := bucket.NewWriter(ctx, filename, opts)
	if err != nil {
		return nil, fmt.Errorf(`error creating writer for bucket "%s": %w`, b.name, err)
	}

	return w, nil
}

func (b *Bucket) createReader(
	ctx context.Context,
	bucket *blob.Bucket,
	filename string,
) (*blob.Reader, error) {
	r, err := bucket.NewReader(ctx, filename, nil)
	if err != nil {
		return nil, fmt.Errorf(`error creating reader for path "%s": %w`, filename, err)
	}

	return r, nil
}

func (p *proxyWriteCloser) Close() error {
	if p.closed {
		return nil
	}
	p.closed = true

	after := p.OnClose()
	defer after()

	return p.WriteCloser.Close()
}

func (p *proxyReadCloser) Close() error {
	if p.closed {
		return nil
	}
	p.closed = true

	after := p.OnClose()
	defer after()

	return p.ReadCloser.Close()
}

func (i *listIterator) Next(ctx context.Context) (bucketly.Item, error) {
	if i.iter == nil {
		bucket, err := i.bucket.openBucket(ctx)
		if err != nil {
			return nil, err
		}
		i.blobBucket = bucket

		prefix := i.name
		if i.name == string(i.bucket.PathSeparator()) {
			prefix = ""
		}

		i.iter = i.blobBucket.List(&blob.ListOptions{
			Prefix:    prefix,
			Delimiter: string(i.bucket.PathSeparator()),
		})
	}

	f, err := i.iter.Next(ctx)
	if err == io.EOF || f == nil {
		i.Close()

		return nil, io.EOF
	}

	item := bucketly.NewItem(i.bucket, f.Key)
	item.SetSize(f.Size)
	item.SetModeTime(f.ModTime)
	item.SetDir(f.IsDir || strings.HasSuffix(f.Key, string(i.bucket.PathSeparator())))

	return item, nil
}

func (i *listIterator) Close() error {
	if i.blobBucket != nil {
		return i.blobBucket.Close()
	}

	return nil
}
