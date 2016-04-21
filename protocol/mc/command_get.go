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
	"io"
	"strconv"
	"strings"

	"github.com/weibocom/wqs/service"
)

const (
	GET_NAME  = "get"
	GETS_NAME = "gets"
)

var (
	VALUE_SPACE = []byte("VALUE ")
	END         = []byte("END\r\n")
	SPACE       = []byte{' '}

	NIL_FLAG  = []byte{byte('0')}
	NIL_VALUE = []byte{}
)

func command_get(qservice service.QueueService, cmdLine []byte, lr LineReader, w io.Writer) (err error) {
	keys := Fields(cmdLine)
	if len(keys) <= 1 {
		_, err = w.Write(ERROR)
		return
	}
	for _, key := range keys[1:] {
		queue := strings.Split(string(key), ".")[0]
		biz := strings.Split(string(key), ".")[1]

		data, e := qservice.ReceiveMsg(queue, biz)
		flag := NIL_FLAG
		if e != nil {
			_, err = w.Write(ENGINE_ERROR_PREFIX)
			_, err = w.Write([]byte(e.Error()))
			_, err = w.Write([]byte("\r\n"))
			return
		}
		if len(data) > 0 {
			w.Write(VALUE_SPACE)
			w.Write(key)
			w.Write(SPACE)
			w.Write(flag)
			w.Write(SPACE)
			w.Write([]byte(strconv.Itoa(len(data))))
			w.Write(CRLF)
			w.Write(data)
			w.Write(CRLF)
		}
	}

	w.Write(END)
	return
}
