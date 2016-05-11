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

type MemcacheCommand func(q queue.Queue, tokens []string, r *bufio.Reader, w *bufio.Writer) error

var (
	// all memcached commands are based on https://github.com/memcached/memcached/blob/master/doc/protocol.txt
	commands    = make(map[string]MemcacheCommand)
	CMD_UNKNOWN = "$cmdUnkown$"
	CMD_NOFIELD = "$cmdNoFields$"
)

func registerCommand(name string, command MemcacheCommand) error {
	if _, exists := commands[name]; exists {
		return fmt.Errorf("command registered %s", name)
	}
	commands[name] = command
	return nil
}
