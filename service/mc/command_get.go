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
	"strings"

	"github.com/juju/errors"
	"github.com/weibocom/wqs/engine/kafka"
	"github.com/weibocom/wqs/engine/queue"
)

const (
	GET_NAME  = "get"
	EGET_NAME = "eget"
)

type pair struct {
	key   string
	queue string
	group string
	id    string
	value []byte
	flag  uint64
}

func init() {
	registerCommand(GET_NAME, commandGet)
	registerCommand(EGET_NAME, commandGet)
}

func commandGet(q queue.Queue, tokens []string, r *bufio.Reader, w *bufio.Writer) error {

	fields := len(tokens)
	if fields < 2 {
		fmt.Fprint(w, ERROR)
		return errors.NotValidf("mc tokens %v ", tokens)
	}
	cmd := tokens[0]
	keyValues := make([]pair, 0)
	for _, key := range tokens[1:] {
		k := strings.Split(key, ".")
		queue := k[0]
		group := defaultGroup
		if len(k) > 1 {
			group = k[0]
			queue = k[1]
		}

		id, data, flag, err := q.RecvMessage(queue, group)
		if err != nil {
			if errors.Cause(err) == kafka.ErrTimeout {
				w.WriteString(END)
			} else {
				fmt.Fprintf(w, "%s %s\r\n", ENGINE_ERROR_PREFIX, err)
			}
			return nil
		}
		// eset data : idlen id data
		if strings.EqualFold(cmd, EGET_NAME) {
			idLen := (byte)(len(id))
			esetData := make([]byte, 0)
			esetData = append(esetData, idLen)
			esetData = append(esetData, ([]byte)(id)...)
			data = append(esetData, data...)
		}

		keyValues = append(keyValues, pair{
			key:   key,
			queue: queue,
			group: group,
			id:    id,
			value: data,
			flag:  flag,
		})
	}

	for _, kv := range keyValues {
		fmt.Fprintf(w, "%s %s %d %d\r\n", VALUE, kv.key, kv.flag, len(kv.value))
		w.Write(kv.value)
		w.WriteString("\r\n")
		if strings.EqualFold(cmd, GET_NAME) {
			q.AckMessage(kv.queue, kv.group, kv.id)
		}
	}
	w.WriteString(END)
	return nil
}
