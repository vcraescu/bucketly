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
	}{
		{
			name: "/test1/test2/test3",
		},
		{
			name: "/test1/test2/test3/",
		},
		{
			name: "/test1",
		},
		{
			name: "test",
		},
		{
			name: "test/",
		},
		{
			name: "///",
		},
		{
			name: "/",
		},
		{
			name: "test///",
		},
		{
			name: "//test///",
		},
		{
			name: "//test///foo//bar///",
		},
		{
			name: "",
		},
		{
			name: "/test  /foo   //",
		},
		{
			name: "/test  /foo   ",
		},
	}

	for i, test := range tests {
		a.Equal(filepath.Base(test.name), bucketly.Base(ps, test.name), i)
	}
}

func TestClean(t *testing.T) {
	ps := ps{}
	a := assert.New(t)
	tests := []struct {
		name string
	}{
		{
			name: "/test1//////test2//test3//test4",
		},
		{
			name: "/test1//////test2/////test3//test4/////",
		},
		{
			name: "",
		},
		{
			name: "  ",
		},
		{
			name: "test1//////test2/////test3//test4/",
		},
		{
			name: "////",
		},
		{
			name: "/",
		},
	}

	for i, test := range tests {
		a.Equal(filepath.Clean(test.name), bucketly.Clean(ps, test.name), i)
	}
}

func TestJoin(t *testing.T) {
	ps := ps{}
	a := assert.New(t)
	tests := []struct {
		elem []string
	}{
		{
			elem: []string{"//path/", "to/", "//join"},
		},
		{
			elem: []string{},
		},
		{
			elem: []string{"test\\", "foo/"},
		},
		{
			elem: []string{"", "  ", "/"},
		},
	}

	for i, test := range tests {
		a.Equal(filepath.Join(test.elem...), bucketly.Join(ps, test.elem...), i)
	}
}
