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

	"github.com/juju/errors"
)

const (
	cmdEack = "eack"
	cmdAck  = "ack"
)

func init() {
	registerCommand(cmdAck, commandAck)
	registerCommand(cmdEack, commandEack)
}

func commandAck(q queue.Queue, tokens []string, r *bufio.Reader, w *bufio.Writer) error {

	noreply := false
	fields := len(tokens)
	if fields != 3 && fields != 4 {
		fmt.Fprint(w, respClientErrorBadCmdFormat)
		return errors.NotValidf("mc tokens %v ", tokens)
	}

	if fields == 4 {
		if tokens[3] != noReply {
			fmt.Fprint(w, respClientErrorBadCmdFormat)
			return errors.NotValidf("mc tokens %v ", tokens)
		}
		noreply = true
	}

	keys := strings.Split(tokens[1], ".")
	group := defaultGroup
	queue := keys[0]
	if len(keys) > 1 {
		group = keys[0]
		queue = keys[1]
	}

	if err := q.AckMessage(queue, group, tokens[2]); err != nil {
		fmt.Fprintf(w, "%s %s\r\n", respEngineErrorPrefix, err)
		return nil
	}

	if noreply {
		return nil
	}

	fmt.Fprint(w, respStored)
	return nil
}

func commandEack(q queue.Queue, tokens []string, r *bufio.Reader, w *bufio.Writer) error {

	var noreply string

	fields := len(tokens)
	if fields != 5 && fields != 6 {
		fmt.Fprint(w, respClientErrorBadCmdFormat)
		return errors.NotValidf("mc tokens %v ", tokens)
	}

	key := tokens[1]
	if fields == 6 {
		noreply = tokens[5]
	}

	// for ack command, flag is unused
	_, err := strconv.ParseUint(tokens[2], 10, 32)
	if err != nil {
		fmt.Fprint(w, respClientErrorBadCmdFormat)
		return errors.Trace(err)
	}

	length, err := strconv.ParseUint(tokens[4], 10, 32)
	if err != nil {
		fmt.Fprint(w, respClientErrorBadCmdFormat)
		return errors.Trace(err)
	}

	id := make([]byte, length)
	_, err = io.ReadAtLeast(r, id, int(length))
	if err != nil {
		fmt.Fprint(w, respClientErrorBadDatachunk)
		return errors.Trace(err)
	}
	r.ReadString('\n')

	keys := strings.Split(key, ".")
	group := defaultGroup
	queue := keys[0]
	if len(keys) > 1 {
		group = keys[0]
		queue = keys[1]
	}

	err = q.AckMessage(queue, group, string(id))
	if err != nil {
		fmt.Fprintf(w, "%s %s\r\n", respEngineErrorPrefix, err)
		return nil
	}

	if noReply == noreply {
		return nil
	}

	fmt.Fprint(w, respStored)
	return nil
}
