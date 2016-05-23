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
	SET_NAME = "set"
)

func init() {
	registerCommand(SET_NAME, commandSet)
}

func commandSet(q queue.Queue, tokens []string, r *bufio.Reader, w *bufio.Writer) error {

	var noreply string

	fields := len(tokens)
	if fields != 5 && fields != 6 {
		fmt.Fprint(w, CLIENT_ERROR_BADCMD_FORMAT)
		return errors.NotValidf("mc tokens %v ", tokens)
	}

	key := tokens[1]
	if fields == 6 {
		noreply = tokens[5]
	}

	flag, err := strconv.ParseUint(tokens[2], 10, 32)
	if err != nil {
		fmt.Fprint(w, CLIENT_ERROR_BADCMD_FORMAT)
		return errors.Trace(err)
	}

	length, err := strconv.ParseUint(tokens[4], 10, 32)
	if err != nil {
		fmt.Fprint(w, CLIENT_ERROR_BADCMD_FORMAT)
		return errors.Trace(err)
	}

	data := make([]byte, length)
	_, err = io.ReadAtLeast(r, data, int(length))
	if err != nil {
		fmt.Fprint(w, CLIENT_ERROR_BAD_DATACHUNK)
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

	_, err = q.SendMessage(queue, group, data, flag)
	if err != nil {
		fmt.Fprintf(w, "%s %s\r\n", ENGINE_ERROR_PREFIX, err)
		return nil
	}

	if NOREPLY == noreply {
		return nil
	}
	fmt.Fprint(w, STORED)
	return nil
}
