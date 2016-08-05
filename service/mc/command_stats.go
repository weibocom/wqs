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
	"runtime"
	"syscall"
	"time"
	"unsafe"

	"github.com/weibocom/wqs/engine/queue"
	"github.com/weibocom/wqs/metrics"
)

const (
	cmdStats = "stats"
)

func init() {
	registerCommand(cmdStats, commandStats)
}

func commandStats(q queue.Queue, tokens []string, r *bufio.Reader, w *bufio.Writer) bool {

	fields := len(tokens)
	if fields != 1 && fields != 2 {
		w.WriteString(respClientErrorBadCmdFormat)
		return true
	}

	if fields == 1 {
		var rusage syscall.Rusage
		syscall.Getrusage(0, &rusage)
		getHits := metrics.GetCounter(metrics.CmdGet)
		getMiss := metrics.GetCounter(metrics.CmdGetMiss)
		setHits := metrics.GetCounter(metrics.CmdSet)
		setMiss := metrics.GetCounter(metrics.CmdSetError)
		conns := metrics.GetCounter(metrics.ReConn)

		fmt.Fprintf(w, "STAT pid %d\r\n", os.Getpid())
		fmt.Fprintf(w, "STAT uptime %d\r\n", q.UpTime())
		fmt.Fprintf(w, "STAT time %d\r\n", time.Now().Unix())
		fmt.Fprintf(w, "STAT version %s\r\n", q.Version())
		fmt.Fprintf(w, "STAT pointer_size %d\r\n", unsafe.Sizeof(r)*8)
		fmt.Fprintf(w, "STAT rusage_user %d.%06d\r\n", rusage.Utime.Sec, rusage.Utime.Usec)
		fmt.Fprintf(w, "STAT rusage_system %d.%06d\r\n", rusage.Stime.Sec, rusage.Stime.Usec)
		fmt.Fprintf(w, "STAT curr_connections %d\r\n", conns)
		fmt.Fprintf(w, "STAT total_connections %d\r\n", metrics.GetCounter(metrics.ToConn))
		fmt.Fprintf(w, "STAT connection_structures %d\r\n", conns)
		fmt.Fprintf(w, "STAT get_cmds %d\r\n", getHits+getMiss)
		fmt.Fprintf(w, "STAT get_hits %d\r\n", getHits)
		fmt.Fprintf(w, "STAT set_cmds %d\r\n", setHits+setMiss)
		fmt.Fprintf(w, "STAT set_hits %d\r\n", setHits)
		fmt.Fprintf(w, "STAT bytes_read %d\r\n", metrics.GetCounter(metrics.BytesRead))
		fmt.Fprintf(w, "STAT bytes_written %d\r\n", metrics.GetCounter(metrics.BytesWriten))
		fmt.Fprintf(w, "STAT threads %d\r\n", runtime.NumGoroutine())
		fmt.Fprint(w, respEnd)
	} else if fields == 2 && tokens[1] == "queue" {
		accumulationInfos, err := q.AccumulationStatus()
		if err != nil {
			fmt.Fprintf(w, "%s %s\r\n", respEngineErrorPrefix, err)
			return false
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
		w.WriteString(respError)
	}
	return false
}
