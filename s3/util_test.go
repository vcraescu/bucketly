package s3_test

import (
	"errors"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcraescu/bucketly/s3"
	"os"
	"testing"
)

type PathSeparableMock struct {
	mock.Mock
}

func (m *PathSeparableMock) PathSeparator() rune {
	return '/'
}

func TestDirectorize(t *testing.T) {
	a := assert.New(t)
	tests := []struct {
		name     string
		expected string
	}{
		{
			name:     "/test1/test2/test3",
			expected: "/test1/test2/test3/",
		},
		{
			name:     "test",
			expected: "test/",
		},
		{
			name:     "test/",
			expected: "test/",
		},
		{
			name:     ".",
			expected: "/",
		},
		{
			name:     "    ",
			expected: "/",
		},
		{
			name:     "   / ",
			expected: "/",
		},
		{
			name:     " .  ",
			expected: "/",
		},
	}

	for i, test := range tests {
		a.Equal(test.expected, s3.Directorize(test.name), i)
	}
}

func TestIsNotFoundError(t *testing.T) {
	a := assert.New(t)
	tests := []struct {
		err      error
		expected bool
	}{
		{
			err:      awserr.NewRequestFailure(nil, 404, ""),
			expected: true,
		},
		{
			err:      awserr.NewRequestFailure(nil, 200, ""),
			expected: false,
		},
		{
			err:      errors.New("test"),
			expected: false,
		},
		{
			err:      os.ErrNotExist,
			expected: false,
		},
	}

	for i, test := range tests {
		a.Equal(test.expected, s3.IsNotExists(test.err), i)
	}
}

func TestIsDirPath(t *testing.T) {
	a := assert.New(t)
	tests := []struct {
		name string
		dir  bool
	}{
		{
			name: "/test1/test2/test3",
		},
		{
			name: ".",
			dir:  true,
		},
		{
			name: "",
			dir:  true,
		},
		{
			name: "   ",
			dir:  true,
		},
		{
			name: "foo/bar/",
			dir:  true,
		},
		{
			name: "foo/bar",
		},
		{
			name: "   . ",
			dir:  true,
		},
		{
			name: "   / ",
			dir:  true,
		},
	}

	for i, test := range tests {
		a.Equal(test.dir, s3.IsDirPath(test.name), i)
	}
}

func TestCleanPath(t *testing.T) {
	a := assert.New(t)

	tests := []struct {
		name     string
		expected string
	}{
		{
			name:     "/test1/test2/test3   ",
			expected: "/test1/test2/test3",
		},
		{
			name:     "  test1/test2/test3/",
			expected: "test1/test2/test3/",
		},
		{
			name:     "",
			expected: "/",
		},
		{
			name:     ".",
			expected: "/",
		},
		{
			name:     "    ",
			expected: "/",
		},
		{
			name:     "foo/bar",
			expected: "foo/bar",
		},
	}

	ps := new(PathSeparableMock)
	for i, test := range tests {
		a.Equal(test.expected, s3.CleanPath(ps, test.name), i)
	}
}

func TestSanitizePath(t *testing.T) {
	a := assert.New(t)

	tests := []struct {
		name     string
		expected string
	}{
		{
			name:     "/test1/test2/test3   ",
			expected: "test1/test2/test3",
		},
		{
			name:     "  test1/test2/../test3/",
			expected: "test1/test3/",
		},
		{
			name:     "...",
			expected: "/",
		},
		{
			name:     ".",
			expected: "/",
		},
		{
			name:     "  .....  ",
			expected: "/",
		},
		{
			name:     "foo/bar...",
			expected: "foo/bar",
		},
	}

	ps := new(PathSeparableMock)
	for i, test := range tests {
		name, err := s3.SanitizePath(ps, test.name)
		if !a.NoError(err) {
			continue
		}
		a.Equal(test.expected, name, i)
	}
}
