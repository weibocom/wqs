package mc

import (
	"bufio"
	"bytes"
	"strings"
	"testing"
)

const (
	testCommond = "set test.11111 1000 1000000 20\r\n12345678901234567890"
)

func BenchmarkBufio(b *testing.B) {

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r := strings.NewReader(testCommond)
		br := bufio.NewReaderSize(r, 4096)
		line, err := br.ReadBytes('\n')
		if err != nil {
			b.Error(err)
		}
		line = line[:len(line)-2]
		if len(line) != 30 {
			b.Errorf("len(len) = %d", len(line))
		}
	}
}

func BenchmarkBufferedLineReader(b *testing.B) {

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r := strings.NewReader(testCommond)
		br := NewBufferedLineReader(r, 4096)
		line, err := br.ReadLine()
		if err != nil {
			b.Error(err)
		}
		if len(line) != 30 {
			b.Errorf("len(len) = %d", len(line))
		}
	}
}

func BenchmarkBytesFields(b *testing.B) {

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		keys := bytes.Fields([]byte(testCommond))
		if len(keys) != 6 {
			b.Errorf("len(keys) = %d", len(keys))
		}
	}
}

func BenchmarkUtilsFields(b *testing.B) {

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		keys := Fields([]byte(testCommond))
		if len(keys) != 5 {
			b.Errorf("len(keys) = %d", len(keys))
		}
	}
}
