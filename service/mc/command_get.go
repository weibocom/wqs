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

	"github.com/weibocom/wqs/engine/kafka"
	"github.com/weibocom/wqs/engine/queue"
)

const (
	cmdGet  = "get"
	cmdEget = "eget"
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
	registerCommand(cmdGet, commandGet)
	registerCommand(cmdEget, commandGet)
}

func commandGet(q queue.Queue, tokens []string, r *bufio.Reader, w *bufio.Writer) bool {

	fields := len(tokens)
	if fields < 2 {
		w.WriteString(respError)
		return true
	}

	cmd := tokens[0]
	pairs := make([]pair, 0, 1)
	for _, key := range tokens[1:] {
		k := strings.SplitN(key, ".", 2)
		queue := k[0]
		group := defaultGroup
		if len(k) == 2 {
			group = k[0]
			queue = k[1]
		}

		id, data, flag, err := q.RecvMessage(queue, group)
		if err != nil {
			if err == kafka.ErrTimeout {
				w.WriteString(respEnd)
			} else {
				fmt.Fprintf(w, "%s %s\r\n", respEngineErrorPrefix, err)
			}
			return false
		}
		// eget data : idlen id data
		if cmd == cmdEget {
			idLen := (byte)(len(id))
			egetData := make([]byte, 0, 1024)
			egetData = append(egetData, idLen)
			egetData = append(egetData, id...)
			data = append(egetData, data...)
		}

		pairs = append(pairs, pair{
			key:   key,
			queue: queue,
			group: group,
			id:    id,
			value: data,
			flag:  flag,
		})
	}

	for _, p := range pairs {
		fmt.Fprintf(w, "%s %s %d %d\r\n", respValue, p.key, p.flag, len(p.value))
		w.Write(p.value)
		w.WriteString("\r\n")
		if cmd == cmdGet {
			q.AckMessage(p.queue, p.group, p.id)
		}
	}
	w.WriteString(respEnd)
	return false
}
