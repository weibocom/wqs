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
	"io"
	"strconv"
	"strings"

	"github.com/weibocom/wqs/engine/queue"
)

const (
	cmdSet  = "set"
	cmdEset = "eset"
)

func init() {
	registerCommand(cmdSet, commandSet)
	registerCommand(cmdEset, commandSet)
}

func commandSet(q queue.Queue, tokens []string, r *bufio.Reader, w *bufio.Writer) bool {

	noreply := false
	fields := len(tokens)
	if fields != 5 && fields != 6 {
		w.WriteString(respClientErrorBadCmdFormat)
		return true
	}
	cmd := tokens[0]
	key := tokens[1]
	if fields == 6 {
		if noReply != tokens[5] {
			w.WriteString(respClientErrorBadCmdFormat)
			return true
		}
		noreply = true
	}

	flag, err := strconv.ParseUint(tokens[2], 10, 32)
	if err != nil {
		w.WriteString(respClientErrorBadCmdFormat)
		return true
	}

	length, err := strconv.ParseUint(tokens[4], 10, 32)
	if err != nil {
		w.WriteString(respClientErrorBadCmdFormat)
		return true
	}

	data := make([]byte, length+2)
	_, err = io.ReadAtLeast(r, data, int(length+2))
	if err != nil {
		w.WriteString(respClientErrorBadDatachunk)
		return true
	}
	if data[length] != '\r' || data[length+1] != '\n' {
		w.WriteString(respClientErrorBadDatachunk)
		return true
	}

	data = data[:length]
	keys := strings.SplitN(key, ".", 2)
	group := defaultGroup
	queue := keys[0]
	if len(keys) == 2 {
		group = keys[0]
		queue = keys[1]
	}

	id, err := q.SendMessage(queue, group, data, flag)
	if err != nil {
		fmt.Fprintf(w, "%s %s\r\n", respEngineErrorPrefix, err)
		return false
	}

	if noreply {
		return false
	}

	// if eset command, return message id
	if cmd == cmdEset {
		fmt.Fprintf(w, "%s\r\n", id)
		return false
	}
	w.WriteString(respStored)
	return false
}
