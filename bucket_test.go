package bucketly_test

import (
	"context"
	"fmt"
	"github.com/google/uuid"
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
	s.bucket = new(s3.Bucket)
	s.newBucket = createS3Bucket
	s.newBucketManager = newS3BucketManager

	suite.Run(t, s)
}

func TestLocalBucketTestSuite(t *testing.T) {
	s := new(BucketTestSuite)
	s.bucket = new(s3.Bucket)
	s.newBucket = createLocalBucket
	s.newBucketManager = newLocalBucketManager

	suite.Run(t, s)
}

func (suite *BucketTestSuite) SetupTest() {
	switch suite.bucket.(type) {
	case *s3.Bucket:
		suite.bucket = suite.newBucket(os.Getenv("AWS_S3_BUCKET"))
	case *local.Bucket:
		name := fmt.Sprintf("/tmp/bucketly-%s", uuid.New().String())
		suite.bucket = suite.newBucket(name)
	}

	suite.manager = suite.newBucketManager(suite.bucket)
}

func (suite *BucketTestSuite) TearDownTest() {
	ctx := context.Background()
	if err := suite.bucket.RemoveAll(ctx, "/"); err != nil {
		panic(err)
	}

	if err := suite.manager.Remove(context.Background()); err != nil {
		panic(err)
	}
}

func (suite *BucketTestSuite) TestMkdir() {
	ctx := context.Background()
	tests := []struct {
		name     string
		dir      string
		expected string
	}{
		{
			name:     "valid folder",
			dir:      "test_mkdir/",
			expected: "test_mkdir/",
		},
		{
			name:     "valid folder without trailing slash",
			dir:      "test_mkdir2",
			expected: "test_mkdir2/",
		},
		{
			name: "root folder",
			dir:  "/",
		},
		{
			name: "dot",
			dir:  ".",
		},
		{
			name: "spaces",
			dir:  "   ",
		},
		{
			name: "empty path",
			dir:  "",
		},
		{
			name: "double dots",
			dir:  "..",
		},
		{
			name: "triple dots",
			dir:  "...",
		},
	}

	for _, test := range tests {
		suite.Run(test.name, func() {
			if !suite.NoError(suite.bucket.Mkdir(ctx, test.dir)) {
				return
			}

			if !suite.NoError(suite.bucket.Mkdir(ctx, test.dir)) {
				return
			}

			fi, err := suite.bucket.Stat(ctx, test.expected)
			if !suite.NoError(err) {
				return
			}

			suite.True(fi.IsDir())
		})
	}
}

func (suite *BucketTestSuite) TestMkdirAll() {
	ctx := context.Background()
	tests := []struct {
		name string
		dir  string
	}{
		{
			name: "valid path",
			dir:  "test_mkdir_all/test2/test3/test4/",
		},
		{
			name: "root path",
			dir:  "/",
		},
		{
			name: "invalid path",
			dir:  "....",
		},
	}

	for i, test := range tests {
		if !suite.NoError(suite.bucket.MkdirAll(ctx, test.dir), i) {
			continue
		}

		fi, err := suite.bucket.Stat(ctx, test.dir)
		if !suite.NoError(err, i) {
			continue
		}
		suite.True(fi.IsDir(), i)
	}
}

func (suite *BucketTestSuite) TestExists() {
	ctx := context.Background()
	tests := []struct {
		name   string
		dir    bool
		exists bool
	}{
		{
			name:   "test_exists/test2/test3/test4/",
			exists: true,
			dir:    true,
		},
		{
			name:   "test_exists/test2/test3/test123.txt",
			exists: true,
		},
		{
			name:   "foo/bar/",
			exists: false,
			dir:    true,
		},
	}

	for i, test := range tests {
		if test.exists {
			if test.dir {
				if !suite.NoError(suite.bucket.MkdirAll(ctx, test.name), i) {
					continue
				}
			} else {
				_, err := suite.bucket.Write(ctx, test.name, []byte("12345"))
				if !suite.NoError(err) {
					continue
				}
			}
		}

		found, err := suite.bucket.Exists(ctx, test.name)
		if !suite.NoError(err, i) {
			continue
		}
		suite.Equal(test.exists, found, i)
	}
}

func (suite *BucketTestSuite) TestReadWrite() {
	ctx := context.Background()
	tests := []struct {
		name     string
		filename string
		content  []byte
	}{
		{
			name:     "root file",
			filename: "test_read_write.txt",
			content:  []byte("12345"),
		},
		{
			name:     "deep file",
			filename: "test_read_write/test2/test3/test123",
			content:  []byte("12345"),
		},
	}
	for _, test := range tests {
		suite.Run(test.name, func() {
			c, err := suite.bucket.Write(ctx, test.filename, test.content)
			if !suite.NoError(err) {
				return
			}

			suite.Equal(len(test.content), c)
			b, err := suite.bucket.Read(ctx, test.filename)
			if !suite.NoError(err) {
				return
			}

			suite.Equal(test.content, b)
		})
	}
}

func (suite *BucketTestSuite) TestCopy2() {
	ctx := context.Background()
	tests := []struct {
		name string
		from string
		to   string
	}{
		{
			name: "root file",
			from: "test_copy2_source.txt",
			to:   "test_copy2_dest.txt",
		},
		{
			name: "deep file",
			from: "test_copy2_source/test1/test2/test3.txt",
			to:   "test_copy2_dest/test1/test2.txt",
		},
	}

	for _, test := range tests {
		suite.Run(test.name, func() {
			_, err := suite.bucket.Write(ctx, test.from, []byte("12345"))
			if !suite.NoError(err) {
				return
			}

			err = suite.bucket.Copy2(ctx, test.from, test.to)
			if !suite.NoError(err) {
				return
			}
		})
	}
}

func (suite *BucketTestSuite) TestCopyAll2() {
	ctx := context.Background()
	tests := []struct {
		name   string
		from   string
		to     string
		create func(from string) error
	}{
		{
			name: "copy file",
			from: "test_copy_all2_source.txt",
			to:   "test_copy_all2_dest.txt",
			create: func(from string) error {
				_, err := suite.bucket.Write(ctx, from, []byte("12345"))

				return err
			},
		},
		{
			name: "copy dir",
			from: "test_copy_all2_source/",
			to:   "test_copy_all2_dest/",
			create: func(from string) error {
				return suite.createDeepDir(ctx, from)
			},
		},
	}

	for _, test := range tests {
		suite.Run(test.name, func() {
			if !suite.NoError(test.create(test.from)) {
				return
			}

			err := suite.bucket.CopyAll2(ctx, test.from, test.to)
			if !suite.NoError(err) {
				return
			}

			found, err := suite.bucket.Exists(ctx, test.to)
			if !suite.True(found) {
				return
			}

			if l, ok := suite.bucket.(bucketly.Listable); ok {
				fromItems, err := getItemsArray(ctx, l, test.from)
				if !suite.NoError(err) {
					return
				}
				toItems, err := getItemsArray(ctx, l, test.to)
				if !suite.NoError(err) {
					return
				}

				suite.Equal(len(fromItems), len(toItems))
			}
		})
	}
}

func (suite *BucketTestSuite) TestRename() {
	ps := string(suite.bucket.PathSeparator())
	ctx := context.Background()
	tests := []struct {
		name     string
		from     string
		to       string
		dir      bool
		expected []string
	}{
		{
			name: "rename file",
			from: "test_rename_src.txt",
			to:   "test_rename_dest.txt",
		},
		{
			name: "rename dir",
			from: "test_rename_dir_src/",
			to:   "test_rename_dir_dest/",
			dir:  true,
			expected: []string{
				"test1/test2/test3/foo32.txt",
				"test1/test3/test4/",
			},
		},
	}

	for i, test := range tests {
		suite.Run(test.name, func() {
			if test.dir {
				err := suite.createDeepDir(ctx, test.from)
				if !suite.NoError(err) {
					return
				}
			} else {
				_, err := suite.bucket.Write(ctx, test.from, []byte("12345"))
				if !suite.NoError(err, i) {
					return
				}
			}

			err := suite.bucket.Rename(ctx, test.from, test.to)
			if !suite.NoError(err, i) {
				return
			}

			found, err := suite.bucket.Exists(ctx, test.to)
			if !suite.NoError(err, i) {
				return
			}
			suite.True(found, i)

			found, err = suite.bucket.Exists(ctx, test.from)
			if !suite.NoError(err, i) {
				return
			}
			suite.False(found, i)

			for _, p := range test.expected {
				found, err := suite.bucket.Exists(ctx, strings.Join([]string{test.to, p}, ps))
				if !suite.NoError(err) {
					continue
				}
				suite.True(found)
			}
		})
	}
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
}

func (suite *BucketTestSuite) TestWalkStop() {
	if _, ok := suite.bucket.(bucketly.Walkable); !ok {
		return
	}

	ctx := context.Background()
	name := "test_walk_skip_dir/"
	err := suite.createDeepDir(ctx, name)
	if !suite.NoError(err) {
		return
	}

	var actual []string

	err = suite.bucket.(bucketly.Walkable).Walk(ctx, name, func(item bucketly.Item, err error) error {
		itemName := strings.TrimRight(item.Name(), string(suite.bucket.PathSeparator()))
		if strings.HasSuffix(itemName, "test1") {
			return bucketly.ErrStopWalk
		}

		actual = append(actual, itemName)

		return nil
	})
	if suite.NoError(err) {
		suite.Empty(actual)
	}
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
	suite.NoError(destBucket.RemoveAll(ctx, dest))

	manager := suite.newBucketManager(destBucket)
	suite.NoError(manager.Remove(context.Background()))
}

func (suite *BucketTestSuite) TestItems() {
	if _, ok := suite.bucket.(bucketly.Listable); !ok {
		return
	}

	ctx := context.Background()
	baseDir := "test_items/"
	if !suite.NoError(suite.createDeepDir(ctx, baseDir)) {
		return
	}

	tests := []struct {
		name     string
		dir      string
		expected []string
	}{
		{
			name:     "first level",
			dir:      "test_items/",
			expected: []string{"test_items/test1"},
		},
		{
			name: "second level",
			dir:  "test_items/test1/",
			expected: []string{
				"test_items/test1/foo1.txt",
				"test_items/test1/foo11.txt",
				"test_items/test1/test2",
				"test_items/test1/test3",
			},
		},
		{
			name: "third level",
			dir:  "test_items/test1/test2/",
			expected: []string{
				"test_items/test1/test2/foo2.txt",
				"test_items/test1/test2/test3",
			},
		},
		{
			name: "fourth level",
			dir:  "test_items/test1/test2/test3/",
			expected: []string{
				"test_items/test1/test2/test3/foo3.txt",
				"test_items/test1/test2/test3/foo31.txt",
				"test_items/test1/test2/test3/foo32.txt",
			},
		},
		{
			name: "file",
			dir:  "test_items/test1/test2/test3/foo3.txt",
			expected: []string{
				"test_items/test1/test2/test3/foo3.txt",
			},
		},
		{
			name: "empty path",
			dir:  "",
			expected: []string{
				"test_items",
			},
		},
		{
			name: "dot path",
			dir:  ".",
			expected: []string{
				"test_items",
			},
		},
		{
			name:     "root path",
			dir:      "/",
			expected: []string{"test_items"},
		},
	}

	for i, test := range tests {
		suite.Run(test.name, func() {
			iter, err := suite.bucket.(bucketly.Listable).Items(test.dir)
			if !suite.NoError(err, i) {
				return
			}

			var actual []string
			for {
				item, err := iter.Next(ctx)
				if err != nil {
					if err == io.EOF || item == nil {
						break
					}

					if !suite.NoError(err, i) {
						break
					}
				}

				actual = append(actual, strings.TrimSuffix(item.Name(), string(suite.bucket.PathSeparator())))
			}

			suite.Equal(test.expected, actual, i)
		})
	}
}

func (suite *BucketTestSuite) TestChmod() {
	ctx := context.Background()
	tests := []struct {
		name string
		mode os.FileMode
		dir  bool
	}{
		{
			name: "chmod_file.txt",
			mode: 0755,
		},
		{
			name: "chmod_dir/",
			mode: 0777,
			dir:  true,
		},
	}

	for i, test := range tests {
		if test.dir {
			if !suite.NoError(suite.bucket.MkdirAll(ctx, test.name), i) {
				continue
			}
		} else {
			_, err := suite.bucket.Write(ctx, test.name, []byte{1, 2, 3})
			if !suite.NoError(err, i) {
				continue
			}
		}

		err := suite.bucket.Chmod(ctx, test.name, test.mode)
		if err != nil {
			if err == bucketly.ErrNotSupported {
				continue
			}

			suite.NoError(err, i)
		}

		item, err := suite.bucket.Stat(ctx, test.name)
		if !suite.NoError(err, i) {
			continue
		}

		suite.Equal(test.mode, item.Mode().Perm(), i)
	}
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

func createS3Bucket(name string) bucketly.Bucket {
	bucket := newS3Bucket(name)
	manager := newS3BucketManager(bucket)
	if err := manager.Create(context.Background()); err != nil {
		panic(err)
	}

	return bucket
}

func createLocalBucket(name string) bucketly.Bucket {
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

	return bucket
}

func getItemsArray(ctx context.Context, l bucketly.Listable, name string) ([]bucketly.Item, error) {
	it, err := l.Items(name)
	if err != nil {
		return nil, err
	}

	var items []bucketly.Item
	for {
		item, err := it.Next(ctx)
		if err != nil {
			if err == io.EOF {
				return items, nil
			}

			return nil, err
		}

		items = append(items, item)
	}
}
