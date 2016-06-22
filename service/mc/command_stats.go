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
	"os"
	"time"
	"unsafe"

	"github.com/weibocom/wqs/engine/queue"

	"github.com/juju/errors"
)

const (
	cmdStats = "stats"
)

func init() {
	registerCommand(cmdStats, commandStats)
}

func commandStats(q queue.Queue, tokens []string, r *bufio.Reader, w *bufio.Writer) error {

	fields := len(tokens)
	if fields != 1 && fields != 2 {
		fmt.Fprint(w, respClientErrorBadCmdFormat)
		return errors.NotValidf("mc tokens %v ", tokens)
	}

	if fields == 1 {
		fmt.Fprintf(w, "STAT pid %d\r\n", os.Getpid())
		fmt.Fprintf(w, "STAT uptime %d\r\n", q.GetUpTime())
		fmt.Fprintf(w, "STAT time %d\r\n", time.Now().Unix())
		fmt.Fprintf(w, "STAT version qservice %s\r\n", q.GetVersion())
		fmt.Fprintf(w, "STAT pointer_size %d\r\n", unsafe.Sizeof(r)*8)
		//curr_connections
		//total_connections
		//rusage_user
		//rusage_system
		//get_cmds
		//get_hits
		//set_cmds
		//set_hits
		//bytes_read
		//bytes_written
		//limit_maxbytes

		fmt.Fprint(w, respEnd)
	} else if fields == 2 && tokens[1] == "queue" {
		accumulationInfos, err := q.AccumulationStatus()
		if err != nil {
			fmt.Fprintf(w, "%s %s\r\n", respEngineErrorPrefix, err)
			return nil
		}
		for _, accumulationInfo := range accumulationInfos {
			fmt.Fprintf(w, "%s %s.%s %d/%d \r\n", "STAT",
				accumulationInfo.Group,
				accumulationInfo.Queue,
				accumulationInfo.Total,
				accumulationInfo.Consumed)
		}
		fmt.Fprint(w, respEnd)
	} else {
		fmt.Fprint(w, respError)
	}
	return nil
}
