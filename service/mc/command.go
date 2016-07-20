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
	"bufio"
	"fmt"

	"github.com/weibocom/wqs/engine/queue"
)

const (
	defaultGroup                = "default"
	noReply                     = "noreply"
	respValue                   = "VALUE"
	respEnd                     = "END\r\n"
	respError                   = "ERROR\r\n"
	respStored                  = "STORED\r\n"
	respClientErrorBadDatachunk = "CLIENT_ERROR bad data chunk\r\n"
	respClientErrorBadCmdFormat = "CLIENT_ERROR bad command line format\r\n"
	respEngineErrorPrefix       = "SERVER_ERROR engine error"
)

//command返回true时，标识发生不能容忍的错误，需要关闭连接，防止将后续有效数据的格式都破坏掉
type memcacheCommand func(q queue.Queue, tokens []string, r *bufio.Reader, w *bufio.Writer) (close bool)

var (
	// all memcached commands are based on https://github.com/memcached/memcached/blob/master/doc/protocol.txt
	commands = make(map[string]memcacheCommand)
)

func registerCommand(name string, command memcacheCommand) {
	if _, exists := commands[name]; exists {
		panic(fmt.Errorf("command duplicate %q", name))
	}
	commands[name] = command
}

func commandUnkown(q queue.Queue, tokens []string, r *bufio.Reader, w *bufio.Writer) bool {
	w.WriteString(respError)
	return true
}
