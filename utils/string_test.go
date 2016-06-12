package utils

import (
	"strings"
	"testing"
)

func TestBytesToString(t *testing.T) {
	old := `{"Name":"TestString"}`
	var data = []byte(old)
	newStr := BytesToString(data)
	if strings.Compare(old, newStr) != 0 {
		t.FailNow()
	}
}

func BenchmarkBytesToString(t *testing.B) {
	old := `{"Name":"TestString"}`
	var data = []byte(old)
	for i := 0; i < t.N; i++ {
		newStr := BytesToString(data)
		_ = newStr
	}
}

func BenchmarkInternalString(t *testing.B) {
	old := `{"Name":"TestString"}`
	var data = []byte(old)
	for i := 0; i < t.N; i++ {
		newStr := string(data)
		_ = newStr
	}
}
