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
	"github.com/weibocom/wqs/engine/queue"
)

const (
	GET_NAME  = "get"
	GETS_NAME = "gets"
)

type pair struct {
	key   string
	value []byte
}

func command_get(q queue.Queue, tokens []string, r *bufio.Reader, w *bufio.Writer) error {

	fields := len(tokens)
	if fields < 2 {
		fmt.Fprint(w, ERROR)
		return errors.NotValidf("mc tokens %v ", tokens)
	}

	keyValues := make([]pair, 0)
	for _, key := range tokens[1:] {
		k := strings.Split(key, ".")
		queue := k[0]
		group := defaultGroup
		if len(k) > 1 {
			group = k[1]
		}

		_, data, err := q.RecvMsg(queue, group)
		if err != nil {
			fmt.Fprintf(w, "%s %s\r\n", ENGINE_ERROR_PREFIX, err)
			return nil
		}

		keyValues = append(keyValues, pair{key: key, value: data})
	}

	for _, kv := range keyValues {
		fmt.Fprintf(w, "%s %s 0 %d\r\n", VALUE, kv.key, len(kv.value))
		w.Write(kv.value)
		w.WriteString("\r\n")
	}
	w.WriteString(END)
	return nil
}
