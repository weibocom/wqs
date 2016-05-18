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

	"github.com/weibocom/wqs/engine/queue"

	"github.com/juju/errors"
)

const (
	STATS_NAME = "stats"
)

func init() {
	registerCommand(STATS_NAME, commandStats)
}

func commandStats(q queue.Queue, tokens []string, r *bufio.Reader, w *bufio.Writer) error {

	fields := len(tokens)
	if fields != 1 && fields != 2 {
		fmt.Fprint(w, CLIENT_ERROR_BADCMD_FORMAT)
		return errors.NotValidf("mc tokens %v ", tokens)
	}

	if fields == 1 {
		// TODO: implement stats command
		fmt.Fprint(w, END)
	} else if fields == 2 && strings.EqualFold(tokens[1], "queue") {
		accumulationInfos, err := q.AccumulationStatus()
		if err != nil {
			fmt.Fprintf(w, "%s %s\r\n", ENGINE_ERROR_PREFIX, err)
			return nil
		}
		for _, accumulationInfo := range accumulationInfos {
			fmt.Fprintf(w, "%s %s.%s %d/%d \r\n", "STAT",
				accumulationInfo.Group,
				accumulationInfo.Queue,
				accumulationInfo.Total,
				accumulationInfo.Consumed)
		}
		fmt.Fprint(w, END)
	} else {
		fmt.Fprint(w, ERROR)
	}
	return nil
}
