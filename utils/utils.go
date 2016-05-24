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

package utils

import (
	"strconv"
	"strings"

	"github.com/juju/errors"
)

func GenTestMessage(length int) string {
	message := ""
	for i := 0; i < length; i++ {
		message += "T"
	}
	return message
}

func GetIntFromArgs(args map[string]interface{}, arg string, defaultVar int) (int, error) {
	if n := args[arg]; n != nil {
		v, err := strconv.Atoi(n.(string))
		return v, errors.Trace(err)
	}
	return defaultVar, nil
}

func ValidParam(str string) bool {
	return !strings.Contains(str, " ") && !BlankString(str)
}

func BlankString(str string) bool {
	if len(strings.TrimSpace(str)) == 0 {
		return true
	} else {
		return false
	}
}

func BytesToUint64(bytes []byte) uint64 {
	u := uint64(0)
	l := len(bytes)
	if l == 0 {
		return uint64(0)
	}
	u = u | uint64(bytes[l-1])
	for i := l - 1; i >= 0; i-- {
		u = u<<8 | uint64(bytes[i])
	}
	return u
}

func Uint64ToBytes(u uint64) []byte {
	bytes := make([]byte, 8)
	bytes[0] = byte(u)
	bytes[1] = byte(u >> 8)
	bytes[2] = byte(u >> 16)
	bytes[3] = byte(u >> 24)
	bytes[4] = byte(u >> 32)
	bytes[5] = byte(u >> 40)
	bytes[6] = byte(u >> 48)
	bytes[7] = byte(u >> 56)
	return bytes
}

type Int32Slice []int32

func (slice Int32Slice) Len() int {
	return len(slice)
}

func (slice Int32Slice) Less(i, j int) bool {
	return slice[i] < slice[j]
}

func (slice Int32Slice) Swap(i, j int) {
	slice[i], slice[j] = slice[j], slice[i]
}
