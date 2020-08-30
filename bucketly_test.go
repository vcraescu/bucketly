package bucketly_test

import (
	"github.com/stretchr/testify/assert"
	"github.com/vcraescu/bucketly"
	"path/filepath"
	"testing"
)

type ps struct {
}

func (s ps) PathSeparator() rune {
	return '/'
}

func TestBase(t *testing.T) {
	ps := ps{}
	a := assert.New(t)

	tests := []struct {
		name string
		path string
	}{
		{
			name: "absolute path",
			path: "/test1/test2/test3",
		},
		{
			name: "absolute path trailing slash",
			path: "/test1/test2/test3/",
		},
		{
			name: "absolute path single folder",
			path: "/test1",
		},
		{
			name: "relative path single folder",
			path: "test",
		},
		{
			name: "relative path single folder with trailing slash",
			path: "test/",
		},
		{
			name: "multiple slashes",
			path: "///",
		},
		{
			name: "root slash",
			path: "/",
		},
		{
			name: "multiple trailing slashes",
			path: "test///",
		},
		{
			name: "multiple prefix slashes",
			path: "//test",
		},
		{
			name: "multiple slashes inside path",
			path: "//test///foo//bar///",
		},
		{
			name: "empty path",
			path: "",
		},
		{
			name: "spaces inside path with trailing slash",
			path: "/test  /foo   //",
		},
		{
			name: "spaces inside path",
			path: "/test  /foo   ",
		},
	}

	for i, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			a.Equal(filepath.Base(test.name), bucketly.Base(ps, test.name), i)
		})
	}
}

func TestClean(t *testing.T) {
	ps := ps{}
	a := assert.New(t)
	tests := []struct {
		name     string
		expected string
	}{
		{
			name:     "/test1//////test2//test3//test4",
			expected: "/test1/test2/test3/test4",
		},
		{
			name:     "/test1//////test2/////test3//test4/////",
			expected: "/test1/test2/test3/test4",
		},
		{
			name:     "",
			expected: ".",
		},
		{
			name:     "  ",
			expected: ".",
		},
		{
			name:     "test1//////test2/////test3//test4/",
			expected: "test1/test2/test3/test4",
		},
		{
			name:     "////",
			expected: "/",
		},
		{
			name:     "/",
			expected: "/",
		},
	}

	for i, test := range tests {
		a.Equal(test.expected, bucketly.Clean(ps, test.name), i)
	}
}

func TestJoin(t *testing.T) {
	ps := ps{}
	tests := []struct {
		name     string
		elem     []string
		expected string
	}{
		{
			name:     "multiple invalid paths",
			elem:     []string{"//path/", "to/", "//join"},
			expected: "/path/to/join",
		},
		{
			name:     "no paths",
			elem:     []string{},
			expected: "",
		},
		{
			name:     "multiple invalid paths 2",
			elem:     []string{"test\\", "foo/"},
			expected: "test\\/foo",
		},
		{
			name:     "with empty paths",
			elem:     []string{"", " ", "/"},
			expected: "/",
		},
		{
			name:     "single dir",
			elem:     []string{"test/"},
			expected: "test",
		},
	}

	for i, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			a := assert.New(t)
			a.Equal(test.expected, bucketly.Join(ps, test.elem...), i)
		})
	}
}

func TestAbs(t *testing.T) {
	ps := ps{}
	a := assert.New(t)
	tests := []struct {
		name     string
		expected string
	}{
		{
			name:     "/foo/bar",
			expected: "/foo/bar",
		},
		{
			name:     "/../../foo/bar",
			expected: "/foo/bar",
		},
		{
			name:     "foo/../bar/",
			expected: "bar",
		},
		{
			name:     "foo/./bar/",
			expected: "foo/bar",
		},
		{
			name:     ".",
			expected: "",
		},
		{
			name:     "",
			expected: "",
		},
		{
			name:     "///",
			expected: "/",
		},
		{
			name:     "..",
			expected: "",
		},
		{
			name:     "...",
			expected: "...",
		},
	}

	for i, test := range tests {
		name, err := bucketly.Abs(ps, test.name)
		if !a.NoError(err, i) {
			continue
		}
		a.Equal(test.expected, name, i)
	}
}

func TestSanitize(t *testing.T) {
	ps := ps{}
	a := assert.New(t)
	tests := []struct {
		name     string
		path     string
		expected string
	}{
		{
			name:     "multiple slashes",
			path:     "/test1//////test2//test3//test4",
			expected: "test1/test2/test3/test4",
		},
		{
			name:     "multple trailing slashes",
			path:     "/test1//////test2/////test3//test4/////",
			expected: "test1/test2/test3/test4",
		},
		{
			name:     "empty",
			path:     "",
			expected: "/",
		},
		{
			name:     "multiple spaces",
			path:     "    ",
			expected: "/",
		},
		{
			name:     "multiple slashes relative path",
			path:     "test1//////test2/////test3//test4/",
			expected: "test1/test2/test3/test4",
		},
		{
			name:     "multiple root slashes",
			path:     "////",
			expected: "/",
		},
		{
			name:     "root",
			path:     "/",
			expected: "/",
		},
		{
			name:     "multiple dots",
			path:     "....",
			expected: "/",
		},
		{
			name:     "double dots inside path",
			path:     "/foo/bar/../baz",
			expected: "foo/baz",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			name, err := bucketly.Sanitize(ps, test.path)
			if !a.NoError(err) {
				return
			}

			a.Equal(test.expected, name)
		})
	}
}
