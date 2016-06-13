package utils

import (
	"reflect"
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

func TestStringToBytes(t *testing.T) {
	str := `{"Name":"TestString"}`
	old := []byte(str)
	newBytes := StringToBytes(str)
	if !reflect.DeepEqual(newBytes, old) {
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

func BenchmarkStringToBytes(t *testing.B) {
	old := `{"Name":"TestString"}`
	for i := 0; i < t.N; i++ {
		newBytes := StringToBytes(old)
		_ = newBytes
	}
}

func BenchmarkInternalBytes(t *testing.B) {
	old := `{"Name":"TestString"}`
	for i := 0; i < t.N; i++ {
		newBytes := []byte(old)
		_ = newBytes
	}
}
