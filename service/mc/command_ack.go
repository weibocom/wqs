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
	cmdEack = "eack"
	cmdAck  = "ack"
)

func init() {
	registerCommand(cmdAck, commandAck)
	registerCommand(cmdEack, commandEack)
}

func commandAck(q queue.Queue, tokens []string, r *bufio.Reader, w *bufio.Writer) bool {

	noreply := false
	fields := len(tokens)
	if fields != 3 && fields != 4 {
		w.WriteString(respClientErrorBadCmdFormat)
		return true
	}

	if fields == 4 {
		if tokens[3] != noReply {
			w.WriteString(respClientErrorBadCmdFormat)
			return true
		}
		noreply = true
	}

	keys := strings.SplitN(tokens[1], ".", 2)
	group := defaultGroup
	queue := keys[0]
	if len(keys) == 2 {
		group = keys[0]
		queue = keys[1]
	}

	if err := q.AckMessage(queue, group, tokens[2]); err != nil {
		fmt.Fprintf(w, "%s %s\r\n", respEngineErrorPrefix, err)
		return false
	}

	if noreply {
		return false
	}

	w.WriteString(respStored)
	return false
}

func commandEack(q queue.Queue, tokens []string, r *bufio.Reader, w *bufio.Writer) bool {

	noreply := false
	fields := len(tokens)
	if fields != 5 && fields != 6 {
		w.WriteString(respClientErrorBadCmdFormat)
		return true
	}

	key := tokens[1]
	if fields == 6 {
		if tokens[5] != noReply {
			w.WriteString(respClientErrorBadCmdFormat)
			return true
		}
		noreply = true
	}

	// for ack command, flag is unused
	_, err := strconv.ParseUint(tokens[2], 10, 32)
	if err != nil {
		w.WriteString(respClientErrorBadCmdFormat)
		return true
	}

	length, err := strconv.ParseUint(tokens[4], 10, 32)
	if err != nil {
		w.WriteString(respClientErrorBadCmdFormat)
		return true
	}

	id := make([]byte, length+2)
	_, err = io.ReadAtLeast(r, id, int(length)+2)
	if err != nil {
		w.WriteString(respClientErrorBadDatachunk)
		return true
	}
	if id[length] != '\r' || id[length+1] != '\n' {
		w.WriteString(respClientErrorBadDatachunk)
		return true
	}

	id = id[:length]
	keys := strings.SplitN(key, ".", 2)
	group := defaultGroup
	queue := keys[0]
	if len(keys) == 2 {
		group = keys[0]
		queue = keys[1]
	}

	if err = q.AckMessage(queue, group, string(id)); err != nil {
		fmt.Fprintf(w, "%s %s\r\n", respEngineErrorPrefix, err)
		return false
	}

	if noreply {
		return false
	}

	w.WriteString(respStored)
	return false
}
