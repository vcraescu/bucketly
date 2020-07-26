package bucketly_test

import (
	"context"
	"github.com/stretchr/testify/suite"
	"github.com/vcraescu/bucketly"
	"github.com/vcraescu/bucketly/local"
	"github.com/vcraescu/bucketly/s3"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"testing"
)

type BucketTestSuite struct {
	suite.Suite

	newBucket        func(name string) bucketly.Bucket
	newBucketManager func(bucket bucketly.Bucket) bucketly.BucketManager
	bucket           bucketly.Bucket
	manager          bucketly.BucketManager
}

func TestS3BucketTestSuite(t *testing.T) {
	s := new(BucketTestSuite)
	bucket := newS3Bucket(os.Getenv("AWS_S3_BUCKET"))
	s.newBucket = newS3Bucket
	s.bucket = bucket

	s.newBucketManager = newS3BucketManager
	s.manager = s3.NewBucketManager(bucket.(*s3.Bucket))

	suite.Run(t, s)
}

func TestLocalBucketTestSuite(t *testing.T) {
	s := new(BucketTestSuite)
	bucket := newLocalBucket("/tmp/test")
	s.newBucket = newLocalBucket
	s.bucket = bucket

	s.newBucketManager = newLocalBucketManager
	s.manager = local.NewBucketManager(bucket.(*local.Bucket))

	suite.Run(t, s)
}

func (suite *BucketTestSuite) TearDownSuite() {
	if err := suite.manager.Remove(context.Background()); err != nil {
		panic(err)
	}
}

func (suite *BucketTestSuite) TestMkdir() {
	ctx := context.Background()
	dir := "test_mkdir/"
	if !suite.NoError(suite.bucket.Mkdir(ctx, dir)) {
		return
	}

	fi, err := suite.bucket.Stat(ctx, dir)
	if !suite.NoError(err) {
		return
	}
	suite.True(fi.IsDir())

	suite.NoError(suite.bucket.Remove(ctx, dir))
}

func (suite *BucketTestSuite) TestMkdirAll() {
	ctx := context.Background()
	dir := "test_mkdir_all/test2/test3/test4/"
	if !suite.NoError(suite.bucket.MkdirAll(ctx, dir)) {
		return
	}

	fi, err := suite.bucket.Stat(ctx, dir)
	if !suite.NoError(err) {
		return
	}
	suite.True(fi.IsDir())
	suite.Equal(dir, fi.Name())

	suite.NoError(suite.bucket.RemoveAll(ctx, "test_mkdir_all/"))
}

func (suite *BucketTestSuite) TestExistsDir() {
	ctx := context.Background()
	dir := "test_dir_exists/test2/test3/test4/"
	if !suite.NoError(suite.bucket.MkdirAll(ctx, dir)) {
		return
	}

	found, err := suite.bucket.Exists(ctx, "test_dir_exists/test2/test3/")
	if !suite.NoError(err) {
		return
	}
	suite.True(found)

	found, err = suite.bucket.Exists(ctx, "test_dir_exists/test2/")
	if !suite.NoError(err) {
		return
	}
	suite.True(found)

	found, err = suite.bucket.Exists(ctx, "test_dir_exists/")
	if !suite.NoError(err) {
		return
	}
	suite.True(found)

	found, err = suite.bucket.Exists(ctx, dir)
	if !suite.NoError(err) {
		return
	}
	suite.True(found)
	suite.NoError(suite.bucket.RemoveAll(ctx, "test_dir_exists/"))
}

func (suite *BucketTestSuite) TestExistsFile() {
	ctx := context.Background()
	name := "test_file_exists/test2/test3/test123.txt"
	_, err := suite.bucket.Write(ctx, name, []byte("12345"))
	if !suite.NoError(err) {
		return
	}

	found, err := suite.bucket.Exists(ctx, name)
	if !suite.NoError(err) {
		return
	}
	suite.True(found)
	suite.NoError(suite.bucket.RemoveAll(ctx, "test_file_exists/"))

	found, err = suite.bucket.Exists(ctx, "this/does/not/exists")
	if !suite.NoError(err) {
		return
	}
	suite.False(found)
}

func (suite *BucketTestSuite) TestReadWrite() {
	ctx := context.Background()
	name := "test123"
	c, err := suite.bucket.Write(ctx, name, []byte("12345"))
	if !suite.NoError(err) {
		return
	}

	suite.Equal(5, c)
	b, err := suite.bucket.Read(ctx, name)
	if !suite.NoError(err) {
		return
	}

	suite.Equal("12345", string(b))
	suite.NoError(suite.bucket.Remove(ctx, name))

	name = "test1/test2/test3/test123"
	c, err = suite.bucket.Write(ctx, name, []byte("12345"))
	if !suite.NoError(err) {
		return
	}

	suite.Equal(5, c)
	b, err = suite.bucket.Read(ctx, name)
	if !suite.NoError(err) {
		return
	}

	suite.Equal("12345", string(b))
	suite.NoError(suite.bucket.RemoveAll(ctx, name))
}

func (suite *BucketTestSuite) TestCopy2() {
	ctx := context.Background()
	from := "test_copy2_source.txt"
	_, err := suite.bucket.Write(ctx, from, []byte("12345"))
	if !suite.NoError(err) {
		return
	}

	to := "test_copy2_dest.txt"
	err = suite.bucket.Copy2(ctx, from, to)
	if !suite.NoError(err) {
		return
	}

	suite.NoError(suite.bucket.Remove(ctx, from))
	suite.NoError(suite.bucket.Remove(ctx, to))

	from = "test_copy2_source/test1/test2/test3.txt"
	_, err = suite.bucket.Write(ctx, from, []byte("12345"))
	if !suite.NoError(err) {
		return
	}

	to = "test_copy2_dest/test1/test2.txt"
	err = suite.bucket.Copy2(ctx, from, to)
	if !suite.NoError(err) {
		return
	}

	suite.NoError(suite.bucket.RemoveAll(ctx, "test_copy2_source/"))
	suite.NoError(suite.bucket.RemoveAll(ctx, "test_copy2_dest/"))
}

func (suite *BucketTestSuite) TestCopyAll2() {
	ctx := context.Background()
	ps := string(suite.bucket.PathSeparator())
	from := "test_copy_all2_source.txt"
	_, err := suite.bucket.Write(ctx, from, []byte("12345"))
	if !suite.NoError(err) {
		return
	}

	to := "test_copy_all2_dest.txt"
	err = suite.bucket.CopyAll2(ctx, from, to)
	if !suite.NoError(err) {
		return
	}

	suite.NoError(suite.bucket.Remove(ctx, from))
	suite.NoError(suite.bucket.Remove(ctx, to))

	from = "test_copy_all2_source/"
	err = suite.createDeepDir(ctx, from)
	if !suite.NoError(err) {
		return
	}

	to = "test_copy_all2_dest/"
	err = suite.bucket.CopyAll2(ctx, from, to)
	if !suite.NoError(err) {
		return
	}

	found, err := suite.bucket.Exists(ctx, bucketly.Join(suite.bucket, to, "test1/test2/test3/foo32.txt"))
	if !suite.NoError(err) {
		return
	}
	suite.True(found)

	found, err = suite.bucket.Exists(ctx, bucketly.Join(suite.bucket, to, "test1/test3/test4/")+ps)
	if !suite.NoError(err) {
		return
	}
	suite.True(found)

	suite.NoError(suite.bucket.RemoveAll(ctx, from))
	suite.NoError(suite.bucket.RemoveAll(ctx, to))
}

func (suite *BucketTestSuite) TestRenameFile() {
	ctx := context.Background()
	from := "test_rename_src.txt"
	_, err := suite.bucket.Write(ctx, from, []byte("12345"))
	if !suite.NoError(err) {
		return
	}

	to := "test_rename_dest.txt"
	err = suite.bucket.Rename(ctx, from, to)
	if !suite.NoError(err) {
		return
	}

	found, err := suite.bucket.Exists(ctx, to)
	if !suite.NoError(err) {
		return
	}
	suite.True(found)

	found, err = suite.bucket.Exists(ctx, from)
	if !suite.NoError(err) {
		return
	}
	suite.False(found)

	suite.NoError(suite.bucket.Remove(ctx, to))
}

func (suite *BucketTestSuite) TestRenameDir() {
	ps := string(suite.bucket.PathSeparator())
	ctx := context.Background()
	from := "test_rename_dir_src/"
	err := suite.createDeepDir(ctx, from)
	if !suite.NoError(err) {
		return
	}

	to := "test_rename_dir_dest/"
	err = suite.bucket.Rename(ctx, from, to)
	if !suite.NoError(err) {
		return
	}

	found, err := suite.bucket.Exists(ctx, bucketly.Join(suite.bucket, to, "test1/test2/test3/foo32.txt"))
	if !suite.NoError(err) {
		return
	}
	suite.True(found)

	found, err = suite.bucket.Exists(ctx, bucketly.Join(suite.bucket, to, "test1/test3/test4/")+ps)
	if !suite.NoError(err) {
		return
	}
	suite.True(found)
	suite.NoError(suite.bucket.RemoveAll(ctx, to))
}

func (suite *BucketTestSuite) TestRemoveAll() {
	ctx := context.Background()
	name := "test_remove_dir_src/"
	err := suite.createDeepDir(ctx, name)
	if !suite.NoError(err) {
		return
	}
	suite.NoError(suite.bucket.RemoveAll(ctx, name))

	name = "test.txt"
	_, err = suite.bucket.Write(ctx, name, []byte("12345"))
	if !suite.NoError(err) {
		return
	}
	suite.NoError(suite.bucket.RemoveAll(ctx, name))
}

func (suite *BucketTestSuite) TestWalk() {
	if _, ok := suite.bucket.(bucketly.Walkable); !ok {
		return
	}

	ctx := context.Background()
	name := "test_walk/"
	err := suite.createDeepDir(ctx, name)
	if !suite.NoError(err) {
		return
	}

	actual := make([]string, 0)
	expected := []string{
		"test_walk/test1",
		"test_walk/test1/foo1.txt",
		"test_walk/test1/foo11.txt",
		"test_walk/test1/test2",
		"test_walk/test1/test2/foo2.txt",
		"test_walk/test1/test2/test3",
		"test_walk/test1/test2/test3/foo3.txt",
		"test_walk/test1/test2/test3/foo31.txt",
		"test_walk/test1/test2/test3/foo32.txt",
		"test_walk/test1/test3",
		"test_walk/test1/test3/test4",
	}
	err = suite.bucket.(bucketly.Walkable).Walk(ctx, name, func(item bucketly.Item, err error) error {
		actual = append(actual, strings.TrimRight(item.Name(), string(suite.bucket.PathSeparator())))

		return nil
	})
	suite.NoError(err)
	suite.Equal(expected, actual)
	suite.NoError(suite.bucket.RemoveAll(ctx, name))

	name = "test_walk.txt"
	_, err = suite.bucket.Write(ctx, name, []byte("12345"))
	if !suite.NoError(err) {
		return
	}

	err = suite.bucket.(bucketly.Walkable).Walk(ctx, name, func(item bucketly.Item, err error) error {
		suite.Equal("test_walk.txt", item.Name())
		return nil
	})
	suite.NoError(err)
	suite.NoError(suite.bucket.RemoveAll(ctx, name))
}

func (suite *BucketTestSuite) TestWalkSkipDir() {
	if _, ok := suite.bucket.(bucketly.Walkable); !ok {
		return
	}

	ctx := context.Background()
	name := "test_walk_skip_dir/"
	err := suite.createDeepDir(ctx, name)
	if !suite.NoError(err) {
		return
	}

	actual := make([]string, 0)
	expected := []string{
		"test_walk_skip_dir/test1",
		"test_walk_skip_dir/test1/foo1.txt",
		"test_walk_skip_dir/test1/foo11.txt",
		"test_walk_skip_dir/test1/test3",
		"test_walk_skip_dir/test1/test3/test4",
	}
	err = suite.bucket.(bucketly.Walkable).Walk(ctx, name, func(item bucketly.Item, err error) error {
		itemName := strings.TrimRight(item.Name(), string(suite.bucket.PathSeparator()))
		if strings.HasSuffix(itemName, "test2") {
			return bucketly.ErrSkipWalkDir
		}

		actual = append(actual, itemName)

		return nil
	})
	if suite.NoError(err) {
		suite.Equal(expected, actual)
	}
	suite.NoError(suite.bucket.RemoveAll(ctx, name))
}

func (suite *BucketTestSuite) TestWalkFile() {
	if _, ok := suite.bucket.(bucketly.Walkable); !ok {
		return
	}

	ctx := context.Background()
	name := "test_walk_file.html"
	_, err := suite.bucket.Write(ctx, name, []byte{1, 2, 3})
	if !suite.NoError(err) {
		return
	}

	err = suite.bucket.(bucketly.Walkable).Walk(ctx, name, func(item bucketly.Item, err error) error {
		suite.True(item.Name() == name)

		return nil
	})
	suite.NoError(err)
	suite.NoError(suite.bucket.Remove(ctx, name))

	err = suite.bucket.(bucketly.Walkable).Walk(ctx, "this_does_not_exists", func(item bucketly.Item, err error) error {
		suite.True(false)

		return nil
	})
	suite.NoError(err)
}

func (suite *BucketTestSuite) TestStatFile() {
	ctx := context.Background()
	name := "test_stat_file.html"

	_, err := suite.bucket.Write(ctx, name, []byte{1, 2, 3})
	if !suite.NoError(err) {
		return
	}

	info, err := suite.bucket.Stat(ctx, name)
	if !suite.NoError(err) {
		return
	}
	suite.NoError(suite.bucket.Remove(ctx, name))

	suite.Equal(name, info.Name())
	suite.False(info.IsDir())
	suite.EqualValues(3, info.Size())
	suite.NotNil(info.Mode())

	_, err = suite.bucket.Stat(ctx, "does_not_exists")
	if suite.Error(err) {
		suite.True(os.IsNotExist(err))
	}
}

func (suite *BucketTestSuite) TestStatDir() {
	ctx := context.Background()
	name := "test_stat_dir/"
	err := suite.createDeepDir(ctx, name)
	if !suite.NoError(err) {
		return
	}

	ps := string(suite.bucket.PathSeparator())
	info, err := suite.bucket.Stat(ctx, bucketly.Join(suite.bucket, name, "test1/test2/test3/")+ps)
	if !suite.NoError(err) {
		return
	}
	suite.NoError(suite.bucket.RemoveAll(ctx, name))

	suite.Equal(bucketly.Join(suite.bucket, name, "test1/test2/test3/")+ps, info.Name())
	suite.True(info.IsDir())
	suite.NotNil(info.Mode())

	_, err = suite.bucket.Stat(ctx, bucketly.Join(suite.bucket, name, "test1/test2/test3/")+ps)
	if suite.Error(err) {
		suite.True(os.IsNotExist(err))
	}
}

func (suite *BucketTestSuite) TestNewReaderFile() {
	ctx := context.Background()
	name := "test_new_reader_file.html"
	_, err := suite.bucket.Write(ctx, name, []byte{1, 2, 3})
	if !suite.NoError(err) {
		return
	}

	r, err := suite.bucket.NewReader(ctx, name)
	if !suite.NoError(err) {
		return
	}
	defer r.Close()

	content, err := ioutil.ReadAll(r)
	suite.NoError(suite.bucket.Remove(ctx, name))
	if !suite.NoError(err) {
		return
	}
	suite.Equal([]byte{1, 2, 3}, content)
}

func (suite *BucketTestSuite) TestNewReaderDir() {
	ctx := context.Background()
	name := "test_new_reader_dir/"
	err := suite.createDeepDir(ctx, name)
	if !suite.NoError(err) {
		return
	}

	_, err = suite.bucket.NewReader(ctx, bucketly.Join(suite.bucket, name, "test1/test2/"))
	suite.Error(err)
	suite.NoError(suite.bucket.RemoveAll(ctx, name))
}

func (suite *BucketTestSuite) TestCopy() {
	ctx := context.Background()
	name := "test_transfer_src.txt"
	_, err := suite.bucket.Write(ctx, name, []byte{1, 2, 3})
	if !suite.NoError(err) {
		return
	}

	destBucket := suite.newBucket("dest")
	dest := "test_transfer_dest.txt"
	suite.NoError(destBucket.Copy(ctx, bucketly.NewItem(suite.bucket, name), dest))
	suite.NoError(suite.bucket.Remove(ctx, name))
	suite.NoError(destBucket.Remove(ctx, dest))

	manager := suite.newBucketManager(destBucket)
	suite.NoError(manager.Remove(context.Background()))
}

func (suite *BucketTestSuite) TestCopyAll() {
	if _, ok := suite.bucket.(bucketly.Walkable); !ok {
		return
	}

	ctx := context.Background()
	name := "test_copy_all_src/"
	if !suite.NoError(suite.createDeepDir(ctx, name)) {
		return
	}

	destBucket := suite.newBucket("dest")
	dest := "test_copy_all_dest/"
	if suite.NoError(destBucket.CopyAll(ctx, bucketly.NewItem(suite.bucket, name), dest)) {
		suite.testWalkDeepDir(destBucket.(bucketly.Walkable), dest)
	}
	suite.NoError(suite.bucket.RemoveAll(ctx, name))
	suite.NoError(destBucket.RemoveAll(ctx, dest))

	manager := suite.newBucketManager(destBucket)
	suite.NoError(manager.Remove(context.Background()))
}

func (suite *BucketTestSuite) TestItems() {
	if _, ok := suite.bucket.(bucketly.Listable); !ok {
		return
	}

	ctx := context.Background()
	name := "test_items/"
	if !suite.NoError(suite.createDeepDir(ctx, name)) {
		return
	}

	tests := []struct {
		name     string
		expected []string
	}{
		{
			name:     "test_items/",
			expected: []string{"test_items/test1"},
		},
		{
			name: "test_items/test1/",
			expected: []string{
				"test_items/test1/foo1.txt",
				"test_items/test1/foo11.txt",
				"test_items/test1/test2",
				"test_items/test1/test3",
			},
		},
		{
			name: "test_items/test1/test2/",
			expected: []string{
				"test_items/test1/test2/foo2.txt",
				"test_items/test1/test2/test3",
			},
		},
		{
			name: "test_items/test1/test2/test3/",
			expected: []string{
				"test_items/test1/test2/test3/foo3.txt",
				"test_items/test1/test2/test3/foo31.txt",
				"test_items/test1/test2/test3/foo32.txt",
			},
		},
		{
			name: "test_items/test1/test2/test3/foo3.txt",
			expected: []string{
				"test_items/test1/test2/test3/foo3.txt",
			},
		},
		{
			name: "",
			expected: []string{
				"test_items",
			},
		},
		{
			name: ".",
			expected: []string{
				"test_items",
			},
		},
		{
			name:     "/",
			expected: []string{"test_items"},
		},
	}

	for i, test := range tests {
		iter, err := suite.bucket.(bucketly.Listable).Items(test.name)
		if !suite.NoError(err, i) {
			continue
		}

		var actual []string
		for {
			item, err := iter.Next(ctx)
			if err != nil {
				if err == io.EOF || item == nil {
					break
				}

				if !suite.NoError(err, i) {
					return
				}
			}

			actual = append(actual, strings.TrimSuffix(item.Name(), string(suite.bucket.PathSeparator())))
		}

		suite.Equal(test.expected, actual, i)
	}

	suite.NoError(suite.bucket.RemoveAll(ctx, name))
}

func (suite *BucketTestSuite) createDeepDir(ctx context.Context, baseDir string) error {
	ps := string(suite.bucket.PathSeparator())
	if err := suite.bucket.MkdirAll(ctx, bucketly.Join(suite.bucket, baseDir, "test1/test2/test3/")+ps); err != nil {
		return err
	}

	if err := suite.bucket.MkdirAll(ctx, bucketly.Join(suite.bucket, baseDir, "test1/test3/test4/")); err != nil {
		return err
	}

	if _, err := suite.bucket.Write(ctx, bucketly.Join(suite.bucket, baseDir, "test1/test2/foo2.txt"), []byte("12345")); err != nil {
		return err
	}

	if _, err := suite.bucket.Write(ctx, bucketly.Join(suite.bucket, baseDir, "test1/foo1.txt"), []byte("12345")); err != nil {
		return err
	}
	if _, err := suite.bucket.Write(ctx, bucketly.Join(suite.bucket, baseDir, "test1/foo11.txt"), []byte("12345")); err != nil {
		return err
	}

	if _, err := suite.bucket.Write(ctx, bucketly.Join(suite.bucket, baseDir, "test1/test2/test3/foo3.txt"), []byte("12345")); err != nil {
		return err
	}

	if _, err := suite.bucket.Write(ctx, bucketly.Join(suite.bucket, baseDir, "test1/test2/test3/foo31.txt"), []byte("12345")); err != nil {
		return err
	}

	if _, err := suite.bucket.Write(ctx, bucketly.Join(suite.bucket, baseDir, "test1/test2/test3/foo32.txt"), []byte("12345")); err != nil {
		return err
	}

	return nil
}

func (suite *BucketTestSuite) testWalkDeepDir(bucket bucketly.Walkable, name string) {
	ctx := context.Background()
	actual := make([]string, 0)
	ps := string(suite.bucket.PathSeparator())
	expected := []string{
		bucketly.Join(suite.bucket, name, "test1/"),
		bucketly.Join(suite.bucket, name, "test1/foo1.txt"),
		bucketly.Join(suite.bucket, name, "test1/foo11.txt"),
		bucketly.Join(suite.bucket, name, "test1/test2/"),
		bucketly.Join(suite.bucket, name, "test1/test2/foo2.txt"),
		bucketly.Join(suite.bucket, name, "test1/test2/test3/"),
		bucketly.Join(suite.bucket, name, "test1/test2/test3/foo3.txt"),
		bucketly.Join(suite.bucket, name, "test1/test2/test3/foo31.txt"),
		bucketly.Join(suite.bucket, name, "test1/test2/test3/foo32.txt"),
		bucketly.Join(suite.bucket, name, "test1/test3/"),
		bucketly.Join(suite.bucket, name, "test1/test3/test4/"),
	}

	err := bucket.Walk(ctx, name, func(item bucketly.Item, err error) error {
		actual = append(actual, strings.TrimSuffix(item.Name(), ps))

		return nil
	})
	if suite.NoError(err) {
		suite.Equal(expected, actual)
	}
}

func newS3Bucket(name string) bucketly.Bucket {
	bucket, err := s3.NewBucket(
		name,
		s3.WithRegion(os.Getenv("AWS_S3_REGION")),
		s3.WithAccessKey(os.Getenv("AWS_S3_ACCESS_KEY")),
		s3.WithSecretAccessKey(os.Getenv("AWS_S3_SECRET_ACCESS_KEY")),
		s3.WithEndpoint(os.Getenv("AWS_S3_ENDPOINT")),
	)
	if err != nil {
		panic(err)
	}

	manager := newS3BucketManager(bucket)
	if err := manager.Create(context.Background()); err != nil {
		panic(err)
	}

	return bucket
}

func newLocalBucket(name string) bucketly.Bucket {
	bucket := local.NewBucket(name)

	manager := newLocalBucketManager(bucket)
	if err := manager.Create(context.Background()); err != nil {
		panic(err)
	}

	return bucket
}

func newS3BucketManager(bucket bucketly.Bucket) bucketly.BucketManager {
	return s3.NewBucketManager(bucket.(*s3.Bucket))
}

func newLocalBucketManager(bucket bucketly.Bucket) bucketly.BucketManager {
	return local.NewBucketManager(bucket.(*local.Bucket))
}
