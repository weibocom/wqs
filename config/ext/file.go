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
package ext

import (
	"bufio"
	"os"
	"strings"
)

type fileConfig struct {
	dir string
	fd  *os.File
}

func (f *fileConfig) Load() (ret map[string]Section, err error) {
	f.fd, err = os.Open(f.dir)
	if err != nil {
		return nil, err
	}

	ret = make(map[string]Section)
	secName := ""
	reader := bufio.NewReader(f.fd)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			break
		}
		line = strings.Trim(line, "\r\n")
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "#") || strings.HasPrefix(line, ";") {
			continue
		}

		// campat
		delIndex := strings.Index(line, ".")
		if delIndex < 1 || len(line) < 5 {
			continue
		}
		secName = line[:delIndex]
		line = line[delIndex+1:]
		// if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {

		if _, ok := ret[secName]; !ok {
			ret[secName] = make(Section)
		}
		if index := strings.Index(line, "="); index > 0 {
			ret[secName][line[:index]] = line[index+1:]
		}
	}
	return
}

func (f *fileConfig) LastModify() int64 {
	info, err := f.fd.Stat()
	if err != nil {
		return 0
	}
	return info.ModTime().Unix()
}
