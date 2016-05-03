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
	"fmt"
	"io"

	"github.com/weibocom/wqs/engine/queue"
)

type MemcacheCommand func(q queue.Queue, cmdLine []byte, lr LineReader, writer io.Writer) error

var (
	// all memcached commands are based on https://github.com/memcached/memcached/blob/master/doc/protocol.txt
	commands    = make(map[string]MemcacheCommand)
	CMD_UNKNOWN = "$cmdUnkown$"
	CMD_NOFIELD = "$cmdNoFields$"
)

func init() {
	registerCommand(GET_NAME, command_get)
	registerCommand(GETS_NAME, command_get)
	registerCommand(SET_NAME, commandSet)
	registerCommand(CMD_UNKNOWN, commandUnkown)
}

func registerCommand(name string, command MemcacheCommand) error {
	if _, exists := commands[name]; exists {
		return fmt.Errorf("command registered %s", name)
	}
	commands[name] = command
	return nil
}
