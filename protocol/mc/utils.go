/*
Copyright 2009-2016 Weibo, Inc.

All files licensed under the Apache License, Version 2.0 (the "License");
you may not use these files except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package mc

import (
	"bytes"
	"io"
)

const (
	CR = '\r'
	LF = '\n'
)

var CRLF = []byte{'\r', '\n'}

type LineReader interface {
	ReadLine() ([]byte, error)
}

type BufferedLineReader struct {
	buff  []byte
	r, w  int
	start int // line start idx
	rd    io.Reader
}

func NewBufferedLineReader(r io.Reader, size int) *BufferedLineReader {
	buf := make([]byte, size)
	return &BufferedLineReader{
		buff:  buf,
		r:     0,
		w:     0,
		start: 0, // line start idx
		rd:    r,
	}
}

func (b *BufferedLineReader) Reset() {
	if b.w > b.r {
		copy(b.buff[0:], b.buff[b.r:b.w])
	}
	b.w = b.w - b.r
	b.r = 0
	b.start = 0
}

func (b *BufferedLineReader) ReadLine() ([]byte, error) {
	var err error
	var cnt int
	for {
		line, found := b.readLineFromBuffer()
		if found {
			return line, nil
		}
		if b.w >= len(b.buff) { // buff is full
			break
		}
		cnt, err = b.rd.Read(b.buff[b.w:])
		b.w += cnt
		if cnt > 0 {
			continue
		}
		return nil, err
	}
	if err != nil { // this error almost be EOF
		return nil, err
	}
	// buff is full and line not found. continue read from reader
	prefix := b.buff[b.start:b.w]
	b.start = b.w // buffer is full
	idx := -1
	eb := make([]byte, len(b.buff))
	for {
		cnt, err = b.rd.Read(eb)
		if cnt > 0 {
			data := eb[0:cnt]
			idx = LocateLineIdx(prefix, data)
			prefix = append(prefix, data...)
			if idx >= 0 {
				break
			}
		} else {
			break
		}
	}
	line := prefix[0:idx]
	if idx+2 < len(prefix) { // more data exists. slice it to the beginning of buff
		copy(b.buff, prefix[idx+2:])
	}
	b.r = 0
	b.w = len(prefix) - (idx + 2)
	b.start = 0
	return line, err
}

func locateLineIdx(data []byte) int {
	dl := len(data)
	if dl < 2 {
		return -1
	}
	crIdx := bytes.IndexByte(data, CR)
	if crIdx >= 0 && crIdx < dl-1 && data[crIdx+1] == LF {
		return crIdx
	}
	return -1
}

// locate the "\r\n" index out of data.
// be carefull the '\r' may be in prefix, and '\n' be in data
func LocateLineIdx(prefix, data []byte) int {
	pl := len(prefix)
	dl := len(data)

	// locate the line in prefix
	preIdx := locateLineIdx(prefix)
	if preIdx >= 0 {
		return preIdx
	}

	if pl > 0 && dl > 0 {
		if prefix[pl-1] == CR && data[0] == LF {
			return pl - 1
		}
	}

	dataIdx := locateLineIdx(data)
	if dataIdx >= 0 {
		return pl + dataIdx
	}
	return -1
}

// read line from buffer.
// if not found. return the prefix
func (b *BufferedLineReader) readLineFromBuffer() ([]byte, bool) {
	prefix := b.buff[b.start:b.r]
	data := b.buff[b.r:b.w]
	idx := LocateLineIdx(prefix, data)
	found := idx >= 0
	if found {
		lstart := b.start
		lend := lstart + idx
		b.start = b.start + idx + 2
		b.r = b.start
		return b.buff[lstart:lend], true
	} else { // all buffer has been read completely
		b.r = b.w
	}
	return nil, false
}

const (
	FIELDS_BUFFER = 8
)

// Fields split data into fields by space ' '.
func Fields(data []byte) [][]byte {
	fields := make([][]byte, 0, FIELDS_BUFFER)
	c := 0
	start := 0
	for {
		idx := bytes.IndexByte(data[start:], ' ')
		if idx >= 0 {
			if idx > 0 { // idx == 0 means the continious space met
				fields = append(fields, data[start:start+idx])
				c++
			}
			start = start + idx + 1
		} else {
			if start < len(data) {
				fields = append(fields, data[start:])
				c++
			}
			break
		}
	}
	return fields[0:c]
}
